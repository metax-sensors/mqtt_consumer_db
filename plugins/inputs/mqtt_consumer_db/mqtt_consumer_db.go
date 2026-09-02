//go:generate ../../../tools/readme_config_includer/generator
package mqtt_consumer_db

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf/plugins/inputs/mqtt_consumer"
	"github.com/influxdata/telegraf/plugins/parsers/json_v2"
	"github.com/influxdata/telegraf/plugins/parsers/value"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/logger"
	"github.com/influxdata/telegraf/models"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sample.conf
var sampleConfig string

const (
	// topicsChannel is the PostgreSQL NOTIFY channel that announces ACL changes.
	// The notification payload carries the client ID whose ACL changed.
	topicsChannel = "mqtt_topics_changed"

	// dbTimeout bounds every individual database round trip.
	dbTimeout = 10 * time.Second

	// listenIdleTimeout is how long the listener waits for a notification
	// before it verifies that the connection is still alive.
	listenIdleTimeout = 60 * time.Second

	// retryMin and retryMax bound the exponential backoff used when the
	// listener connection or the MQTT consumer has to be re-established.
	retryMin = 1 * time.Second
	retryMax = 30 * time.Second
)

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

	parser telegraf.Parser
	acc    telegraf.Accumulator
	pool   *pgxpool.Pool

	// Lifecycle of the topic listener goroutine.
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// consumerMu serializes Start/Stop of the embedded mqtt_consumer, which
	// can be triggered by Start, Gather, Stop and the listener goroutine.
	consumerMu       sync.Mutex
	consumerDown     bool
	startBackoff     time.Duration
	nextStartAttempt time.Time
}

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

// fetchTopics retrieves the subscribe ACL (Access Control List) for the
// configured client ID from the database and returns the topics the client is
// allowed to subscribe to, minus the ones matching topic_exclude.
func (m *MQTTConsumerDB) fetchTopics(ctx context.Context) ([]string, error) {
	if m.pool == nil {
		return nil, errors.New("database pool not initialized")
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	clientID := m.Mqtt_Consumer.ClientID
	m.debug_log("fetching subscribe ACL for client_id=%q", clientID)

	var acl *string
	err := m.pool.QueryRow(ctx, "SELECT subscribe_acl FROM vmq_auth_acl WHERE client_id = $1", clientID).Scan(&acl)
	if err != nil {
		return nil, fmt.Errorf("querying subscribe ACL for client %q: %w", clientID, err)
	}
	if acl == nil {
		m.Log.Warnf("Client %q has no subscribe ACL, nothing to subscribe to", clientID)
		return nil, nil
	}

	topics, err := parseSubscribeACL(*acl)
	if err != nil {
		return nil, fmt.Errorf("client %q: %w", clientID, err)
	}

	result := m.filterTopics(topics)
	m.debug_log("Topics: %v (excluded %d)", result, len(topics)-len(result))
	return result, nil
}

// parseSubscribeACL decodes the JSON subscribe ACL stored by the broker into
// the list of topic patterns it grants.
func parseSubscribeACL(raw string) ([]string, error) {
	var entries []subscribe_structure
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("decoding subscribe ACL: %w", err)
	}

	topics := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Topic == "" {
			continue
		}
		topics = append(topics, entry.Topic)
	}
	return topics, nil
}

// filterTopics drops every topic that matches one of the topic_exclude
// patterns.
func (m *MQTTConsumerDB) filterTopics(topics []string) []string {
	result := make([]string, 0, len(topics))
	for _, topic := range topics {
		excluded := false
		for _, pattern := range m.TopicExclude {
			if matchTopic(pattern, topic) {
				m.debug_log("excluding topic %q (matched pattern %q)", topic, pattern)
				excluded = true
				break
			}
		}
		if !excluded {
			result = append(result, topic)
		}
	}
	return result
}

// listen keeps a notification listener on the database for as long as ctx is
// alive, re-establishing the connection with backoff whenever it fails.
func (m *MQTTConsumerDB) listen(ctx context.Context) {
	defer m.wg.Done()
	m.debug_log("listener started")

	delay := retryMin
	for {
		err := m.listenOnce(ctx)
		if ctx.Err() != nil {
			m.debug_log("listener stopped")
			return
		}

		m.error_log("topic listener failed, reconnecting in %s: %v", delay, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, retryMax)
	}
}

// listenOnce holds one database connection, subscribes to the notification
// channel and refreshes the topics on every notification for this client. It
// returns when the connection fails or ctx is cancelled.
func (m *MQTTConsumerDB) listenOnce(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in listener: %v\n%s", r, debug.Stack())
		}
	}()

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring listener connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+topicsChannel); err != nil {
		return fmt.Errorf("listening on channel %q: %w", topicsChannel, err)
	}
	m.Log.Infof("Listening for topic changes on channel %q", topicsChannel)

	// Notifications sent while the listener was disconnected are gone, so
	// resynchronize the topics once after every (re)connect.
	m.refreshTopics(ctx)

	for {
		waitCtx, cancelWait := context.WithTimeout(ctx, listenIdleTimeout)
		notification, err := conn.Conn().WaitForNotification(waitCtx)
		cancelWait()

		switch {
		case err == nil:
			m.debug_log("notification received channel=%q payload=%q", notification.Channel, notification.Payload)
			if notification.Channel == topicsChannel && notification.Payload == m.Mqtt_Consumer.ClientID {
				m.refreshTopics(ctx)
			}
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, context.DeadlineExceeded):
			// Nothing arrived for a while. A silently dropped connection would
			// never deliver another notification, so verify it is alive.
			pingCtx, cancelPing := context.WithTimeout(ctx, dbTimeout)
			err = conn.Ping(pingCtx)
			cancelPing()
			if err != nil {
				return fmt.Errorf("listener connection lost: %w", err)
			}
		default:
			return fmt.Errorf("waiting for notification: %w", err)
		}
	}
}

// refreshTopics reloads the topics from the database and restarts the MQTT
// consumer if they changed. Failures keep the current subscriptions.
func (m *MQTTConsumerDB) refreshTopics(ctx context.Context) {
	topics, err := m.fetchTopics(ctx)
	if err != nil {
		m.error_log("refreshing subscription topics failed, keeping current topics: %v", err)
		return
	}

	m.consumerMu.Lock()
	defer m.consumerMu.Unlock()

	if slices.Equal(topics, m.Mqtt_Consumer.Topics) {
		m.debug_log("subscription topics unchanged")
		return
	}

	m.Log.Infof("Subscription topics changed, restarting MQTT consumer with %d topics", len(topics))
	m.Mqtt_Consumer.Stop()
	m.Mqtt_Consumer.Topics = topics
	if err := m.startConsumerLocked(); err != nil {
		m.error_log("restarting MQTT consumer failed, retrying in %s: %v", m.startBackoff, err)
	}
}

func (*MQTTConsumerDB) SampleConfig() string {
	return sampleConfig
}

// newEmbeddedMQTTConsumer builds the embedded consumer through Telegraf's
// plugin registry. Upstream's constructor is unexported since Telegraf 1.34,
// and it is the only place that sets the MQTT client factory and the defaults
// for max_undelivered_messages, keepalive, timeouts and reconnect interval. A
// bare struct literal leaves the factory nil, which panics on the first connect.
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
	if m.Log == nil {
		m.Log = logger.New("inputs", "mqtt_consumer_db", m.ServerID)
	}
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

	// Create database connection pool. Connections are established lazily.
	dsn := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(username, password),
		Host:   m.Server,
		Path:   "/" + m.Database,
	}
	m.debug_log("connecting to postgres server=%q database=%q", m.Server, m.Database)
	pool, err := pgxpool.New(context.Background(), dsn.String())
	if err != nil {
		m.error_log("unable to connect to database: %v", err)
		return fmt.Errorf("creating database connection pool: %w", err)
	}
	m.pool = pool

	// recreate instances
	if m.Mqtt_Consumer == nil {
		m.Mqtt_Consumer, err = newEmbeddedMQTTConsumer()
		if err != nil {
			return fmt.Errorf("initializing embedded mqtt_consumer failed: %w", err)
		}
	}
	// mqtt_consumer logs from Init on, so the logger has to be in place here.
	m.Mqtt_Consumer.Log = levelFilterLogger{Logger: m.Log}

	if m.Mqtt_Consumer.ClientID == "" {
		return errors.New("mqtt_consumer.client_id is required: it selects the subscription ACL and the change notifications")
	}

	// Initialize parser based on data_format
	m.debug_log("selecting parser for data_format=%q", m.DataFormat)
	switch m.DataFormat {
	case "json_v2":
		if m.JSON_v2 == nil {
			m.JSON_v2 = &json_v2.Parser{}
		}
		m.parser = m.JSON_v2
		m.debug_log("using json_v2 parser")
	default:
		dataType := m.DataType
		if dataType == "" {
			dataType = "float"
		}
		m.parser = &value.Parser{
			MetricName: "mqtt_consumer_db",
			DataType:   dataType,
		}
		m.debug_log("using value parser with data_type=%q", dataType)
	}

	// The parser decoded from the toml struct field never receives a logger.
	// json_v2 dereferences its Log field on several error paths, so a missing
	// logger turns a bad payload into a nil-pointer panic.
	models.SetLoggerOnPlugin(m.parser, m.Log)

	if initializer, ok := m.parser.(interface{ Init() error }); ok {
		if err := initializer.Init(); err != nil {
			m.error_log("initializing %T parser failed: %v", m.parser, err)
			return fmt.Errorf("initializing %T parser failed: %w", m.parser, err)
		}
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
	if m.pool == nil {
		m.error_log("db pool is nil in Start")
		return errors.New("db pool not initialized")
	}

	m.acc = &CustomAccumulator{Accumulator: acc, Debug: m.Debug, ServerID: m.ServerID, DataFormat: m.DataFormat} // save the accumulator in case we need to restart the plugin

	// The mqtt_consumer plugin won't work without a parser. The parser runs
	// inside paho's message callback where a panic would take down the whole
	// process, so hand over a panic-guarded wrapper.
	m.Mqtt_Consumer.SetParser(newSafeParser(m.parser))
	m.debug_log("mqtt_consumer client_id=%q servers=%v", m.Mqtt_Consumer.ClientID, m.Mqtt_Consumer.Servers)

	ctx, cancel := context.WithCancel(context.Background())

	// Without the initial topic list there is nothing to subscribe to, so a
	// database failure at this point is a startup error.
	topics, err := m.fetchTopics(ctx)
	if err != nil {
		cancel()
		m.error_log("error creating topics: %v", err)
		return fmt.Errorf("loading subscription topics: %w", err)
	}
	m.Mqtt_Consumer.Topics = topics
	m.debug_log("loaded %d topics for client_id=%q", len(topics), m.Mqtt_Consumer.ClientID)

	// Start the listener
	m.cancel = cancel
	m.wg.Add(1)
	go m.listen(ctx)

	// Start the MQTT consumer. A broker that is not reachable yet is not
	// fatal: Gather keeps retrying with backoff.
	m.consumerMu.Lock()
	defer m.consumerMu.Unlock()
	if err := m.startConsumerLocked(); err != nil {
		m.error_log("mqtt_consumer start failed, retrying in %s: %v", m.startBackoff, err)
		return nil
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

	// Stop the listener first so it cannot restart the consumer concurrently.
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.wg.Wait()

	m.consumerMu.Lock()
	if m.Mqtt_Consumer != nil {
		m.Mqtt_Consumer.Stop()
	}
	m.consumerDown = false
	m.consumerMu.Unlock()

	if m.pool != nil {
		m.pool.Close()
		m.pool = nil
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

	m.consumerMu.Lock()
	defer m.consumerMu.Unlock()

	if m.consumerDown {
		if time.Now().Before(m.nextStartAttempt) {
			return nil
		}
		if err := m.startConsumerLocked(); err != nil {
			return fmt.Errorf("restarting MQTT consumer failed, retrying in %s: %w", m.startBackoff, err)
		}
		m.Log.Infof("MQTT consumer restarted")
	}

	err := m.Mqtt_Consumer.Gather(acc)
	if err != nil {
		return fmt.Errorf("gathering metrics failed: %w", err)
	}
	return nil
}

// startConsumerLocked starts the embedded mqtt_consumer and tracks whether a
// retry is needed. The caller must hold consumerMu.
func (m *MQTTConsumerDB) startConsumerLocked() error {
	if err := m.Mqtt_Consumer.Start(m.acc); err != nil {
		m.consumerDown = true
		m.startBackoff = min(max(2*m.startBackoff, retryMin), retryMax)
		m.nextStartAttempt = time.Now().Add(m.startBackoff)
		return err
	}
	m.consumerDown = false
	m.startBackoff = 0
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
