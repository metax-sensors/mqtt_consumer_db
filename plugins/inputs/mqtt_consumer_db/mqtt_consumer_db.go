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
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf/plugins/inputs/mqtt_consumer"
	"github.com/influxdata/telegraf/plugins/parsers/json_v2"
	"github.com/influxdata/telegraf/plugins/parsers/value"

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
	DataFormat    string                      `toml:"data_format"`
	DataType      string                      `toml:"data_type"`
	JSON_v2       *json_v2.Parser             `toml:"json_v2"`
	ServerID      string                      `toml:"server_id"`
	Debug         bool                        `toml:"debug"`
	TopicExclude  []string                    `toml:"topic_exclude"`
	Log           telegraf.Logger             `toml:"-"`

	parser        telegraf.Parser
	acc           telegraf.Accumulator
	wg            sync.WaitGroup
	db_connection *pgxpool.Conn
	db_pool       *pgxpool.Pool
	ctx           context.Context
	cancel        context.CancelFunc
}

/*const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}*/

// matchTopic matches a topic against a pattern split by "/".
// "*" matches exactly one segment, "**" matches zero or more segments.
func matchTopic(pattern, topic string) bool {
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")
	return matchSegments(pParts, tParts)
}

func matchSegments(pattern, topic []string) bool {
	for len(pattern) > 0 {
		p := pattern[0]
		if p == "**" {
			// "**" at the end matches everything remaining
			if len(pattern) == 1 {
				return true
			}
			// try matching rest of pattern at every position
			for i := 0; i <= len(topic); i++ {
				if matchSegments(pattern[1:], topic[i:]) {
					return true
				}
			}
			return false
		}
		if len(topic) == 0 {
			return false
		}
		if p != "*" && p != topic[0] {
			return false
		}
		pattern = pattern[1:]
		topic = topic[1:]
	}
	return len(topic) == 0
}

type subscribe_structure struct {
	Topic string `json:"pattern"`
}

func (m *MQTTConsumerDB) debug_log(formatted_text string, args ...any) {
	if m != nil && m.Debug {
		msg := fmt.Sprintf(formatted_text, args...)
		prefix := fmt.Sprintf("[mqtt_consumer_db:%s]", m.ServerID)
		fmt.Fprintf(os.Stderr, "%s %s\n", prefix, msg)
	}
}

func (m *MQTTConsumerDB) error_log(formatted_text string, args ...any) {
	msg := fmt.Sprintf(formatted_text, args...)
	prefix := fmt.Sprintf("[mqtt_consumer_db:%s]", m.ServerID)
	fmt.Fprintf(os.Stderr, "%s %s\n", prefix, msg)
}

// create_topics retrieves the subscribe ACL (Access Control List) for a given client ID from the database
// and returns a list of topics that the client is allowed to subscribe to or an error if the database query
// or unmarshaling fails.
func (m *MQTTConsumerDB) create_topics(client_id string) ([]string, error) {
	if m.db_connection == nil {
		return nil, errors.New("db connection is nil")
	}

	query := fmt.Sprintf("SELECT subscribe_acl FROM vmq_auth_acl WHERE client_id='%s';", client_id)
	m.debug_log("create_topics for client_id=%q", client_id)

	var subscribe_acl string
	err := m.db_connection.QueryRow(context.Background(), query).Scan(&subscribe_acl)
	if err != nil {
		m.error_log("create_topics query failed for client_id=%q: %v", client_id, err)
		return nil, fmt.Errorf("QueryRow failed: %w", err)
	}

	topics := []subscribe_structure{}
	json.Unmarshal([]byte(subscribe_acl), &topics)

	result := []string{}
	for _, topic := range topics {
		excluded := false
		for _, pattern := range m.TopicExclude {
			if matchTopic(pattern, topic.Topic) {
				m.debug_log("excluding topic %q (matched pattern %q)", topic.Topic, pattern)
				excluded = true
				break
			}
		}
		if !excluded {
			result = append(result, topic.Topic)
		}
	}

	m.debug_log("Topics: %v (excluded %d)", result, len(topics)-len(result))

	return result, nil
}

// listen to a PostgreSQL notification channel and updates the topics
// for the MQTT consumer when a notification is received.
func (m *MQTTConsumerDB) listen() {
	defer func() {
		if r := recover(); r != nil {
			m.error_log("panic in listen: %v\n%s", r, debug.Stack())
		}
	}()
	defer m.wg.Done()

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.debug_log("listener started")

	if m.db_pool == nil {
		m.error_log("db pool is nil in listen")
		return
	}

	// Listen to the channel with its own connection
	conn, err := m.db_pool.Acquire(context.Background())
	if err != nil {
		m.error_log("error acquiring listener connection: %v", err)
		return
	}

	_, err = conn.Exec(context.Background(), "LISTEN mqtt_topics_changed;")
	if err != nil {
		m.error_log("error listening to channel mqtt_topics_changed: %v", err)
		return
	}
	m.debug_log("LISTEN mqtt_topics_changed active")

	defer conn.Release()

	m.debug_log("entering listen loop, waiting for pg notifications...")
	for {
		m.debug_log("WaitForNotification blocking at %v", time.Now().UTC().Format(time.RFC3339))
		notify, err := conn.Conn().WaitForNotification(m.ctx)
		if err != nil {
			m.error_log("wait for notification failed: %v", err)
		}

		// Check if the notification is for the current client (the server itself)
		// and if so, update the topics
		m.debug_log("Topic Update received at %v", time.Now().UTC().Format(time.RFC3339))
		if m.Mqtt_Consumer == nil {
			m.error_log("mqtt_consumer is nil in listener loop")
			continue
		}

		if notify != nil {
			m.debug_log("notification received channel=%q payload=%q", notify.Channel, notify.Payload)
		}

		if notify != nil && notify.Channel == "mqtt_topics_changed" && notify.Payload == m.Mqtt_Consumer.ClientID {
			m.debug_log("[listen] received notification on channel %q with payload %q", notify.Channel, notify.Payload)

			m.Mqtt_Consumer.Topics, err = m.create_topics(m.Mqtt_Consumer.ClientID)
			if err != nil {
				m.error_log("[listen] error creating topics: %v", err)
			} else {
				m.debug_log("[listen] restarting mqtt consumer with %d topics", len(m.Mqtt_Consumer.Topics))
				m.Mqtt_Consumer.Stop()
				if err := m.Mqtt_Consumer.Start(m.acc); err != nil {
					m.error_log("[listen] restart failed: %v", err)
				}
			}
		}

		select {
		case <-m.ctx.Done():
			m.debug_log("context done. Close Listener.")
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
	m.debug_log("init mqtt_consumer_db (server_id=%q, data_format=%q, data_type=%q)", m.ServerID, m.DataFormat, m.DataType)

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
	m.debug_log("connecting to postgres server=%q database=%q", m.Server, m.Database)
	conn, err := pgxpool.New(context.Background(), url)
	if err != nil {
		m.error_log("unable to connect to database: %v", err)
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	m.db_pool = conn

	// recreate instances
	if m.Mqtt_Consumer == nil {
		m.Mqtt_Consumer, err = newEmbeddedMQTTConsumer()
		if err != nil {
			return fmt.Errorf("initializing embedded mqtt_consumer failed: %w", err)
		}
	}

	// Initialize parser based on data_format
	m.debug_log("selecting parser for data_format=%q", m.DataFormat)
	switch m.DataFormat {
	case "json_v2":
		if m.JSON_v2 == nil {
			m.JSON_v2 = &json_v2.Parser{}
		}
		if err := m.JSON_v2.Init(); err != nil {
			m.error_log("initializing json_v2 parser failed: %v", err)
			return fmt.Errorf("initializing json_v2 parser failed: %w", err)
		}
		m.parser = m.JSON_v2
		m.debug_log("using json_v2 parser")
	default:
		dataType := m.DataType
		if dataType == "" {
			dataType = "float"
		}
		p := &value.Parser{
			MetricName: "mqtt_consumer_db",
			DataType:   dataType,
		}
		if err := p.Init(); err != nil {
			m.error_log("initializing value parser failed: %v", err)
			return fmt.Errorf("initializing value parser failed: %w", err)
		}
		m.parser = p
		m.debug_log("using value parser with data_type=%q", dataType)
	}

	m.debug_log("parser type: %T", m.parser)

	err = m.Mqtt_Consumer.Init()
	if err != nil {
		m.error_log("initializing mqtt_consumer plugin failed: %v", err)
		return fmt.Errorf("initializing mqtt_consumer plugin failed: %w", err)
	}
	m.debug_log("init complete")

	return nil
}

func (m *MQTTConsumerDB) Start(acc telegraf.Accumulator) (startErr error) {
	defer func() {
		if r := recover(); r != nil {
			startErr = fmt.Errorf("panic in Start: %v", r)
			m.error_log("%v\n%s", startErr, debug.Stack())
		}
	}()

	m.debug_log("start called (parser=%T)", m.parser)
	if m.Mqtt_Consumer == nil {
		m.error_log("mqtt_consumer not configured")
		return errors.New("mqtt_consumer not configured")
	}

	if m.parser == nil {
		m.error_log("parser not configured")
		return errors.New("parser not configured")
	}
	if m.db_pool == nil {
		m.error_log("db pool is nil in Start")
		return errors.New("db pool not initialized")
	}

	m.acc = &CustomAccumulator{Accumulator: acc, Debug: m.Debug, ServerID: m.ServerID, DataFormat: m.DataFormat} // save the accumulator in case we need to restart the plugin
	m.Mqtt_Consumer.Log = levelFilterLogger{Logger: m.Log}

	m.Mqtt_Consumer.SetParser(m.parser) // set the parser in the mqtt_consumer plugin
	// important, because the mqtt_consumer plugin
	// won't work without a parser
	m.debug_log("mqtt_consumer client_id=%q servers=%v", m.Mqtt_Consumer.ClientID, m.Mqtt_Consumer.Servers)

	// Acquire a connection from the pool to create the topics
	pool_conn, err := m.db_pool.Acquire(context.Background())
	if err != nil {
		m.error_log("unable to acquire connection: %v", err)
		return fmt.Errorf("Unable to acquire connection: %w", err)
	}
	m.db_connection = pool_conn

	m.Mqtt_Consumer.Topics, err = m.create_topics(m.Mqtt_Consumer.ClientID)

	if err != nil {
		m.error_log("error creating topics: %v", err)
		return fmt.Errorf("Error creating topics: %w", err)
	}
	m.debug_log("loaded %d topics for client_id=%q", len(m.Mqtt_Consumer.Topics), m.Mqtt_Consumer.ClientID)

	// Start the listener
	m.wg.Add(1)
	go m.listen()

	// Start the MQTT consumer
	if err := m.Mqtt_Consumer.Start(m.acc); err != nil {
		m.error_log("mqtt_consumer start failed: %v", err)
		return err
	}
	m.debug_log("mqtt_consumer started")
	return nil
}

func (m *MQTTConsumerDB) Stop() {
	defer func() {
		if r := recover(); r != nil {
			m.error_log("panic in Stop: %v\n%s", r, debug.Stack())
		}
	}()

	m.debug_log("stop called")
	if m.Mqtt_Consumer != nil {
		m.Mqtt_Consumer.Stop()
	}
	// Stop the listener
	if m.cancel != nil {
		m.cancel()
	}
	if m.db_connection != nil {
		m.db_connection.Release()
		m.db_connection = nil
	}
	if m.db_pool != nil {
		m.db_pool.Close()
		m.db_pool = nil
	}
	m.debug_log("stop complete")
}

func (m *MQTTConsumerDB) Gather(acc telegraf.Accumulator) (gatherErr error) {
	defer func() {
		if r := recover(); r != nil {
			gatherErr = fmt.Errorf("panic in Gather: %v", r)
			m.error_log("%v\n%s", gatherErr, debug.Stack())
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
	}
}

func init() {
	inputs.Add("mqtt_consumer_db", func() telegraf.Input {
		return New()
	})
}
