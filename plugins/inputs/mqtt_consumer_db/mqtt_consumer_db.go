//go:generate ../../../tools/readme_config_includer/generator
package mqtt_consumer_db

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/influxdata/telegraf/plugins/inputs/mqtt_consumer"
	"github.com/influxdata/telegraf/plugins/parsers/json_v2"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sample.conf
var sampleConfig string

var once sync.Once

type MQTTConsumerDB struct {
	Server        string                      `toml:"db_server"`
	Database      string                      `toml:"db_name"`
	Username      config.Secret               `toml:"db_username"`
	Password      config.Secret               `toml:"db_password"`
	Mqtt_Consumer *mqtt_consumer.MQTTConsumer `toml:"mqtt_consumer"`
	Parser        *json_v2.Parser             `toml:"json_v2"`
	ServerID      string                      `toml:"server_id"`
	Debug         bool                        `toml:"debug"`
	Log           telegraf.Logger             `toml:"-"`

	parser telegraf.Parser
	acc    telegraf.Accumulator
}

var (
	wg            sync.WaitGroup
	db_connection *pgxpool.Conn
	db_pool       *pgxpool.Pool
	instance      *MQTTConsumerDB
	ctx           context.Context
	cancel        context.CancelFunc
)

/*const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}*/

type subscribe_structure struct {
	Topic string `json:"pattern"`
}

func debug_log(formatted_text string, args ...any) {
	if instance.Debug {
		fmt.Fprintf(os.Stderr, fmt.Sprintf(formatted_text+"\n", args...))
	}
}

// create_topics retrieves the subscribe ACL (Access Control List) for a given client ID from the database
// and returns a list of topics that the client is allowed to subscribe to or an error if the database query
// or unmarshaling fails.
func create_topics(client_id string) ([]string, error) {
	query := fmt.Sprintf("SELECT subscribe_acl FROM vmq_auth_acl WHERE client_id='%s';", client_id)

	var subscribe_acl string
	err := db_connection.QueryRow(context.Background(), query).Scan(&subscribe_acl)
	if err != nil {
		return nil, fmt.Errorf("QueryRow failed: %w", err)
	}

	topics := []subscribe_structure{}
	json.Unmarshal([]byte(subscribe_acl), &topics)

	result := []string{}
	for _, topic := range topics {
		result = append(result, topic.Topic)
	}

	debug_log("Topics: %v", result)

	return result, nil
}

// listen to a PostgreSQL notification channel and updates the topics
// for the MQTT consumer when a notification is received.
func listen() {
	defer wg.Done()

	ctx, cancel = context.WithCancel(context.Background())

	// Listen to the channel with its own connection
	conn, err := db_pool.Acquire(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error acquiring connection: %w\n", err)
		os.Exit(1)
	}

	_, err = conn.Exec(context.Background(), "LISTEN mqtt_topics_changed;")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listening to channel: %w\n", err)
		os.Exit(1)
	}

	defer conn.Release()

	for {
		notify, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}

		// Check if the notification is for the current client (the server itself)
		// and if so, update the topics
		debug_log("Topic Update")
		if notify != nil && notify.Channel == "mqtt_topics_changed" && notify.Payload == instance.Mqtt_Consumer.ClientID {
			debug_log("[listen] received notification on channel %q with payload %q", notify.Channel, notify.Payload)

			instance.Mqtt_Consumer.Topics, err = create_topics(instance.Mqtt_Consumer.ClientID)
			if err != nil {
				debug_log("[listen] error creating topics: %w", err)
			} else {
				instance.Mqtt_Consumer.Stop()
				instance.Mqtt_Consumer.Start(instance.acc)
			}
		}

		select {
		case <-ctx.Done():
			debug_log("context done. Close Listener.")
			conn.Conn().Close(context.Background())
			return
		default:
		}
	}
}

func (*MQTTConsumerDB) SampleConfig() string {
	return sampleConfig
}

func (m *MQTTConsumerDB) SetParser(parser telegraf.Parser) {
	m.parser = parser
}

func (m *MQTTConsumerDB) Description() string {
	return "Reads metrics from MQTT topic(s)"
}

func (m *MQTTConsumerDB) Init() error {
	instance = m
	debug_log("Init MQTTConsumerDB")

	// Build the connection string
	var username, password string
	if !m.Username.Empty() {
		user, err := m.Username.Get()
		if err != nil {
			return fmt.Errorf("error getting username: %w", err)
		}
		username = user.String()
		user.Destroy()
	}
	if !m.Password.Empty() {
		pass, err := m.Password.Get()
		if err != nil {
			return fmt.Errorf("error getting password: %w", err)
		}
		password = pass.String()
		pass.Destroy()
	}

	// Create database connection pool
	url := fmt.Sprintf("postgresql://%s:%s@%s/%s", username, password, m.Server, m.Database)
	conn, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	db_pool = conn

	// recreate instances
	if m.Mqtt_Consumer == nil {
		m.Mqtt_Consumer = &mqtt_consumer.MQTTConsumer{}
		m.Parser = &json_v2.Parser{}
	}

	// Initialize mqtt_consumer and parser
	err = m.Parser.Init()
	if err != nil {
		return fmt.Errorf("initializing parser failed: %w", err)
	}

	err = m.Mqtt_Consumer.Init()
	if err != nil {
		return fmt.Errorf("initializing mqtt_consumer plugin failed: %w", err)
	}

	return nil
}

func (m *MQTTConsumerDB) Start(acc telegraf.Accumulator) error {
	m.acc = &CustomAccumulator{acc} // save the accumulator in case we need to restart the plugin
	m.Mqtt_Consumer.Log = m.Log
	m.parser = m.Parser

	if m.parser == nil {
		return errors.New("parser not set")
	}
	m.Mqtt_Consumer.SetParser(m.parser) // set the parser in the mqtt_consumer plugin
	// important, because the mqtt_consumer plugin
	// won't work without a parser

	// Acquire a connection from the pool to create the topics
	pool_conn, err := db_pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("Unable to acquire connection: %w", err)
	}
	db_connection = pool_conn

	m.Mqtt_Consumer.Topics, err = create_topics(m.Mqtt_Consumer.ClientID)

	if err != nil {
		return fmt.Errorf("Error creating topics: %w", err)
	}

	// Start the listener
	wg.Add(1)
	go listen()

	// Start the MQTT consumer
	return m.Mqtt_Consumer.Start(instance.acc)
}

func (m *MQTTConsumerDB) Stop() {
	m.Mqtt_Consumer.Stop()
	// Stop the listener
	cancel()
	if db_connection != nil {
		//defer db_connection.Close(context.Background())
		db_connection.Release()
	}
}

func (m *MQTTConsumerDB) Gather(acc telegraf.Accumulator) error {
	err := m.Mqtt_Consumer.Gather(acc)
	if err != nil {
		return fmt.Errorf("gathering metrics failed: %w", err)
	}
	return nil
}

func New() *MQTTConsumerDB {
	return &MQTTConsumerDB{
		Mqtt_Consumer: &mqtt_consumer.MQTTConsumer{},
		Parser:        &json_v2.Parser{},
	}
}

func init() {
	inputs.Add("mqtt_consumer_db", func() telegraf.Input {
		return New()
	})
}
