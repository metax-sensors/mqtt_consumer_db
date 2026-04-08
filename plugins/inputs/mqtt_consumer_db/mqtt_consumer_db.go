//go:generate ../../../tools/readme_config_includer/generator
package mqtt_consumer_db

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
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
	if instance != nil && instance.Debug {
		msg := fmt.Sprintf(formatted_text, args...)
		if instance.Log != nil {
			instance.Log.Debugf("[mqtt_consumer_db] %s", msg)
			return
		}
		fmt.Fprintf(os.Stderr, "[mqtt_consumer_db] %s\n", msg)
	}
}

func error_log(formatted_text string, args ...any) {
	msg := fmt.Sprintf(formatted_text, args...)
	if instance != nil && instance.Log != nil {
		instance.Log.Errorf("[mqtt_consumer_db] %s", msg)
		return
	}
	fmt.Fprintf(os.Stderr, "[mqtt_consumer_db] %s\n", msg)
}

// create_topics retrieves the subscribe ACL (Access Control List) for a given client ID from the database
// and returns a list of topics that the client is allowed to subscribe to or an error if the database query
// or unmarshaling fails.
func create_topics(client_id string) ([]string, error) {
	if db_connection == nil {
		return nil, errors.New("db connection is nil")
	}

	query := fmt.Sprintf("SELECT subscribe_acl FROM vmq_auth_acl WHERE client_id='%s';", client_id)
	debug_log("create_topics for client_id=%q", client_id)

	var subscribe_acl string
	err := db_connection.QueryRow(context.Background(), query).Scan(&subscribe_acl)
	if err != nil {
		error_log("create_topics query failed for client_id=%q: %v", client_id, err)
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
	defer func() {
		if r := recover(); r != nil {
			error_log("panic in listen: %v\n%s", r, debug.Stack())
		}
	}()
	defer wg.Done()

	ctx, cancel = context.WithCancel(context.Background())
	debug_log("listener started")

	if db_pool == nil {
		error_log("db pool is nil in listen")
		return
	}

	// Listen to the channel with its own connection
	conn, err := db_pool.Acquire(context.Background())
	if err != nil {
		error_log("error acquiring listener connection: %v", err)
		return
	}

	_, err = conn.Exec(context.Background(), "LISTEN mqtt_topics_changed;")
	if err != nil {
		error_log("error listening to channel mqtt_topics_changed: %v", err)
		return
	}
	debug_log("LISTEN mqtt_topics_changed active")

	defer conn.Release()

	for {
		notify, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			error_log("wait for notification failed: %v", err)
		}

		// Check if the notification is for the current client (the server itself)
		// and if so, update the topics
		debug_log("Topic Update")
		if instance == nil || instance.Mqtt_Consumer == nil {
			error_log("instance or mqtt_consumer is nil in listener loop")
			continue
		}

		if notify != nil {
			debug_log("notification received channel=%q payload=%q", notify.Channel, notify.Payload)
		}

		if notify != nil && notify.Channel == "mqtt_topics_changed" && notify.Payload == instance.Mqtt_Consumer.ClientID {
			debug_log("[listen] received notification on channel %q with payload %q", notify.Channel, notify.Payload)

			instance.Mqtt_Consumer.Topics, err = create_topics(instance.Mqtt_Consumer.ClientID)
			if err != nil {
				error_log("[listen] error creating topics: %v", err)
			} else {
				debug_log("[listen] restarting mqtt consumer with %d topics", len(instance.Mqtt_Consumer.Topics))
				instance.Mqtt_Consumer.Stop()
				if err := instance.Mqtt_Consumer.Start(instance.acc); err != nil {
					error_log("[listen] restart failed: %v", err)
				}
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

func newEmbeddedMQTTConsumer() (*mqtt_consumer.MQTTConsumer, error) {
	creator, ok := inputs.Inputs["mqtt_consumer"]
	if !ok {
		return nil, errors.New("mqtt_consumer input plugin is not registered")
	}

	created := creator()
	mqttInput, ok := created.(*mqtt_consumer.MQTTConsumer)
	if !ok {
		return nil, fmt.Errorf("mqtt_consumer creator returned unexpected type %T", created)
	}

	return mqttInput, nil
}

func (m *MQTTConsumerDB) SetParser(parser telegraf.Parser) {
	m.parser = parser
}

func (m *MQTTConsumerDB) Description() string {
	return "Reads metrics from MQTT topic(s)"
}

func (m *MQTTConsumerDB) Init() error {
	instance = m
	debug_log("init mqtt_consumer_db")

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
	debug_log("connecting to postgres server=%q database=%q", m.Server, m.Database)
	conn, err := pgxpool.New(context.Background(), url)
	if err != nil {
		error_log("unable to connect to database: %v", err)
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	db_pool = conn

	// recreate instances
	if m.Mqtt_Consumer == nil {
		m.Mqtt_Consumer, err = newEmbeddedMQTTConsumer()
		if err != nil {
			return fmt.Errorf("initializing embedded mqtt_consumer failed: %w", err)
		}
		m.Parser = &json_v2.Parser{}
	}

	// Initialize mqtt_consumer and parser
	err = m.Parser.Init()
	if err != nil {
		error_log("initializing parser failed: %v", err)
		return fmt.Errorf("initializing parser failed: %w", err)
	}

	err = m.Mqtt_Consumer.Init()
	if err != nil {
		error_log("initializing mqtt_consumer plugin failed: %v", err)
		return fmt.Errorf("initializing mqtt_consumer plugin failed: %w", err)
	}
	debug_log("init complete")

	return nil
}

func (m *MQTTConsumerDB) Start(acc telegraf.Accumulator) (startErr error) {
	defer func() {
		if r := recover(); r != nil {
			startErr = fmt.Errorf("panic in Start: %v", r)
			error_log("%v\n%s", startErr, debug.Stack())
		}
	}()

	debug_log("start called")
	if m.Mqtt_Consumer == nil {
		error_log("mqtt_consumer not configured")
		return errors.New("mqtt_consumer not configured")
	}

	if m.Parser == nil {
		error_log("json_v2 parser not configured")
		return errors.New("json_v2 parser not configured")
	}
	if db_pool == nil {
		error_log("db pool is nil in Start")
		return errors.New("db pool not initialized")
	}

	m.acc = &CustomAccumulator{acc} // save the accumulator in case we need to restart the plugin
	m.Mqtt_Consumer.Log = levelFilterLogger{Logger: m.Log}
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
		error_log("unable to acquire connection: %v", err)
		return fmt.Errorf("Unable to acquire connection: %w", err)
	}
	db_connection = pool_conn

	m.Mqtt_Consumer.Topics, err = create_topics(m.Mqtt_Consumer.ClientID)

	if err != nil {
		error_log("error creating topics: %v", err)
		return fmt.Errorf("Error creating topics: %w", err)
	}
	debug_log("loaded %d topics for client_id=%q", len(m.Mqtt_Consumer.Topics), m.Mqtt_Consumer.ClientID)

	// Start the listener
	wg.Add(1)
	go listen()

	// Start the MQTT consumer
	if err := m.Mqtt_Consumer.Start(instance.acc); err != nil {
		error_log("mqtt_consumer start failed: %v", err)
		return err
	}
	debug_log("mqtt_consumer started")
	return nil
}

func (m *MQTTConsumerDB) Stop() {
	defer func() {
		if r := recover(); r != nil {
			error_log("panic in Stop: %v\n%s", r, debug.Stack())
		}
	}()

	debug_log("stop called")
	if m.Mqtt_Consumer != nil {
		m.Mqtt_Consumer.Stop()
	}
	// Stop the listener
	if cancel != nil {
		cancel()
	}
	if db_connection != nil {
		//defer db_connection.Close(context.Background())
		db_connection.Release()
		db_connection = nil
	}
	if db_pool != nil {
		db_pool.Close()
		db_pool = nil
	}
	debug_log("stop complete")
}

func (m *MQTTConsumerDB) Gather(acc telegraf.Accumulator) (gatherErr error) {
	defer func() {
		if r := recover(); r != nil {
			gatherErr = fmt.Errorf("panic in Gather: %v", r)
			error_log("%v\n%s", gatherErr, debug.Stack())
		}
	}()

	if m.Mqtt_Consumer == nil {
		return errors.New("mqtt_consumer is nil in Gather")
	}

	err := m.Mqtt_Consumer.Gather(acc)
	if err != nil {
		return fmt.Errorf("gathering metrics failed: %w", err)
	}
	return nil
}

func New() *MQTTConsumerDB {
	consumer, err := newEmbeddedMQTTConsumer()
	if err != nil {
		consumer = &mqtt_consumer.MQTTConsumer{}
	}

	return &MQTTConsumerDB{
		Mqtt_Consumer: consumer,
		Parser:        &json_v2.Parser{},
	}
}

func init() {
	inputs.Add("mqtt_consumer_db", func() telegraf.Input {
		return New()
	})
}
