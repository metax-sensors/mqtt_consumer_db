package mqtt_consumer_db

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
)

// jsonConfig mirrors a plugin.conf instance using the json_v2 parser. The
// broker address points at a closed port so connection attempts fail fast.
const jsonConfig = `
[[inputs.mqtt_consumer_db]]
  db_server = "localhost:5432"
  db_name = "telegraf"
  db_username = "telegraf"
  db_password = "telegraf"
  server_id = "test"
  data_format = "json_v2"
  topic_exclude = ["org/*/*/internal", "**/debug"]
  [inputs.mqtt_consumer_db.json_v2]
    [[inputs.mqtt_consumer_db.json_v2.json_v2]]
      measurement_name = "dot"
      [[inputs.mqtt_consumer_db.json_v2.json_v2.object]]
        path = "@this"
        optional = false
        timestamp_key = "ts"
        timestamp_format = "unix_ms"
        timestamp_timezone = "UTC"
  [inputs.mqtt_consumer_db.mqtt_consumer]
    servers = ["tcp://127.0.0.1:1"]
    topic_tag = "topic"
    client_id = "server1_readonly"
`

// valueConfig mirrors the default configuration: single float payloads.
const valueConfig = `
[[inputs.mqtt_consumer_db]]
  db_server = "localhost:5432"
  db_name = "telegraf"
  server_id = "test"
  [inputs.mqtt_consumer_db.mqtt_consumer]
    servers = ["tcp://127.0.0.1:1"]
    topic_tag = "topic"
    client_id = "server1_readonly"
`

// decodePlugin decodes cfg the same way main.go does.
func decodePlugin(t *testing.T, cfg string) *MQTTConsumerDB {
	t.Helper()
	var conf struct {
		Inputs struct {
			MQTTConsumerDB []toml.Primitive `toml:"mqtt_consumer_db"`
		} `toml:"inputs"`
	}
	md, err := toml.Decode(cfg, &conf)
	if err != nil {
		t.Fatalf("toml.Decode: %v", err)
	}
	if len(conf.Inputs.MQTTConsumerDB) != 1 {
		t.Fatalf("expected one instance, got %d", len(conf.Inputs.MQTTConsumerDB))
	}
	m := New()
	if err := md.PrimitiveDecode(conf.Inputs.MQTTConsumerDB[0], m); err != nil {
		t.Fatalf("PrimitiveDecode: %v", err)
	}
	m.Log = &testLogger{}
	return m
}

// loadPlugin decodes cfg and runs Init, registering Stop as cleanup.
func loadPlugin(t *testing.T, cfg string) *MQTTConsumerDB {
	t.Helper()
	m := decodePlugin(t, cfg)
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(m.Stop)
	return m
}

func TestInitAttachesLoggerToParser(t *testing.T) {
	m := loadPlugin(t, jsonConfig)
	if m.JSON_v2.Log == nil {
		t.Fatal("json_v2 parser has no logger after Init")
	}
	if m.parser != telegraf.Parser(m.JSON_v2) {
		t.Fatalf("expected the json_v2 parser to be active, got %T", m.parser)
	}
}

// TestConsumerHasUpstreamDefaults guards against building the embedded
// consumer as a bare struct, which leaves its client factory nil and all
// defaults at zero.
func TestConsumerHasUpstreamDefaults(t *testing.T) {
	m := loadPlugin(t, jsonConfig)
	if m.Mqtt_Consumer.MaxUndeliveredMessages != 1000 {
		t.Fatalf("max_undelivered_messages = %d, want upstream default 1000", m.Mqtt_Consumer.MaxUndeliveredMessages)
	}
	if time.Duration(m.Mqtt_Consumer.KeepAliveInterval) != 60*time.Second {
		t.Fatalf("keepalive = %s, want upstream default 60s", time.Duration(m.Mqtt_Consumer.KeepAliveInterval))
	}
	if time.Duration(m.Mqtt_Consumer.MaxReconnectInterval) != 30*time.Second {
		t.Fatalf("max_reconnect_interval = %s, want upstream default 30s", time.Duration(m.Mqtt_Consumer.MaxReconnectInterval))
	}
}

func TestInitRequiresClientID(t *testing.T) {
	cfg := strings.Replace(jsonConfig, `client_id = "server1_readonly"`, "", 1)
	m := decodePlugin(t, cfg)
	err := m.Init()
	if err == nil || !strings.Contains(err.Error(), "client_id") {
		t.Fatalf("expected a client_id error, got %v", err)
	}
}

func TestParseMalformedPayloadsDoNotPanic(t *testing.T) {
	m := loadPlugin(t, jsonConfig)
	parser := newSafeParser(m.parser)

	cases := []struct {
		payload string
		metrics int
		wantErr bool
	}{
		{`{"ts":1700000000000,"x":1}`, 1, false},
		{`42`, 1, false},
		{`{"ts":"abc","x":1}`, 0, false}, // nil-logger panic before the fix
		{`{"ts":null,"x":1}`, 0, false},  // nil-logger panic before the fix
		{`[{"ts":"bad"}]`, 0, false},     // nil-logger panic before the fix
		{`abc`, 0, true},
		{``, 0, true},
		{`{}`, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.payload, func(t *testing.T) {
			metrics, err := parser.Parse([]byte(tc.payload))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(metrics) != tc.metrics {
				t.Fatalf("got %d metrics, want %d", len(metrics), tc.metrics)
			}
		})
	}
}

func TestValueParserIsDefault(t *testing.T) {
	m := loadPlugin(t, valueConfig)
	parser := newSafeParser(m.parser)

	metrics, err := parser.Parse([]byte("23.5"))
	if err != nil || len(metrics) != 1 {
		t.Fatalf("float payload: metrics=%d err=%v", len(metrics), err)
	}
	if v, ok := metrics[0].GetField("value"); !ok || v != 23.5 {
		t.Fatalf("value field = %v, want 23.5", v)
	}

	metrics, err = parser.Parse([]byte("not a number"))
	if err == nil || len(metrics) != 0 {
		t.Fatalf("garbage payload: metrics=%d err=%v, want an error", len(metrics), err)
	}
}

func TestParseSubscribeACL(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{"topics", `[{"pattern":"org/prod/1/#"},{"pattern":"org/prod/2/#"}]`, []string{"org/prod/1/#", "org/prod/2/#"}, false},
		{"empty list", `[]`, []string{}, false},
		{"skips empty pattern", `[{"pattern":""},{"pattern":"a/b"}]`, []string{"a/b"}, false},
		{"ignores other keys", `[{"pattern":"a/b","qos":1}]`, []string{"a/b"}, false},
		{"invalid json", `not json`, nil, true},
		{"wrong shape", `{"pattern":"a/b"}`, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSubscribeACL(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterTopicsAppliesExcludePatterns(t *testing.T) {
	m := decodePlugin(t, jsonConfig) // topic_exclude = ["org/*/*/internal", "**/debug"]
	topics := []string{
		"org/prod/1/data",
		"org/prod/1/internal",
		"org/prod/1/sub/debug",
		"other/x/debug",
		"org/prod/internal",
	}
	got := m.filterTopics(topics)
	want := "org/prod/1/data,org/prod/internal"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern, topic string
		want           bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b", false},
		{"a/*/c", "a/x/c", true},
		{"a/*/c", "a/x/y/c", false},
		{"a/**", "a", true},
		{"a/**", "a/x/y/z", true},
		{"**/c", "a/b/c", true},
		{"**/c", "a/b/d", false},
		{"a/**/c", "a/c", true},
		{"a/**/c", "a/x/y/c", true},
	}
	for _, tc := range cases {
		if got := matchTopic(tc.pattern, tc.topic); got != tc.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", tc.pattern, tc.topic, got, tc.want)
		}
	}
}

// TestConsumerStartRetriesWithBackoff checks that an unreachable broker does
// not fail the plugin: the consumer is marked down, Gather retries after the
// backoff, and no error escapes while the backoff is still running.
func TestConsumerStartRetriesWithBackoff(t *testing.T) {
	m := loadPlugin(t, jsonConfig)
	m.acc = &CustomAccumulator{Accumulator: newTestAccumulator(m), ServerID: m.ServerID, DataFormat: m.DataFormat}
	m.Mqtt_Consumer.SetParser(newSafeParser(m.parser))
	m.Mqtt_Consumer.Topics = []string{"a/#"}

	m.consumerMu.Lock()
	err := m.startConsumerLocked()
	m.consumerMu.Unlock()
	if err == nil {
		t.Fatal("expected connecting to a closed port to fail")
	}
	if !m.consumerDown || m.consumerRunning || m.startBackoff != retryMin {
		t.Fatalf("consumerDown = %v, consumerRunning = %v, startBackoff = %s; want true, false, %s",
			m.consumerDown, m.consumerRunning, m.startBackoff, retryMin)
	}

	// Still inside the backoff window: Gather must stay quiet.
	if err := m.Gather(m.acc); err != nil {
		t.Fatalf("Gather during backoff returned %v", err)
	}

	// Force the next attempt: it fails again and doubles the backoff.
	m.nextStartAttempt = time.Now().Add(-time.Second)
	if err := m.Gather(m.acc); err == nil {
		t.Fatal("expected Gather to report the failed restart")
	}
	if m.startBackoff != 2*retryMin {
		t.Fatalf("startBackoff = %s, want %s", m.startBackoff, 2*retryMin)
	}
}

// TestStartWithoutDatabaseKeepsRunning covers an instance whose database is
// not reachable at startup. Start must succeed so the other instances in the
// process keep running, the consumer stays unstarted until the listener has
// loaded the topics, and Stop must tear the listener down cleanly.
func TestStartWithoutDatabaseKeepsRunning(t *testing.T) {
	m := decodePlugin(t, jsonConfig)
	m.Server = "127.0.0.1:1" // closed port: fails fast
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(m.Stop)

	if err := m.Start(newTestAccumulator(m)); err != nil {
		t.Fatalf("Start with unreachable database returned %v, want nil", err)
	}
	if m.consumerRunning || m.consumerDown {
		t.Fatalf("consumerRunning = %v, consumerDown = %v; want false, false", m.consumerRunning, m.consumerDown)
	}
	if m.Mqtt_Consumer.Topics != nil {
		t.Fatalf("Topics = %v, want none until the database is reachable", m.Mqtt_Consumer.Topics)
	}
	if m.cancel == nil {
		t.Fatal("listener not started")
	}

	// Gather must not try to start a consumer that has no topics yet.
	if err := m.Gather(m.acc); err != nil {
		t.Fatalf("Gather returned %v", err)
	}
	m.Stop()
	if m.cancel != nil || m.pool != nil {
		t.Fatal("Stop did not release listener and pool")
	}
}

func TestStopWithoutStartIsSafe(t *testing.T) {
	m := decodePlugin(t, jsonConfig)
	m.Stop() // before Init: no pool, no listener, no consumer client

	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	m.Stop()
	m.Stop() // idempotent
}

type panickingParser struct{}

func (panickingParser) Parse([]byte) ([]telegraf.Metric, error)   { panic("boom") }
func (panickingParser) ParseLine(string) (telegraf.Metric, error) { panic("boom") }
func (panickingParser) SetDefaultTags(map[string]string)          {}

func TestSafeParserRecoversFromPanic(t *testing.T) {
	p := newSafeParser(panickingParser{})

	metrics, err := p.Parse([]byte("payload"))
	if err == nil || metrics != nil {
		t.Fatalf("Parse: expected error and no metrics, got %v, %v", metrics, err)
	}
	if got := err.Error(); got != `parser panicked: boom (payload: "payload")` {
		t.Fatalf("unexpected error text: %s", got)
	}

	m, err := p.ParseLine("line")
	if err == nil || m != nil {
		t.Fatalf("ParseLine: expected error and no metric, got %v, %v", m, err)
	}
}

var errSentinel = errors.New("sentinel")

type errorParser struct{}

func (errorParser) Parse([]byte) ([]telegraf.Metric, error)   { return nil, errSentinel }
func (errorParser) ParseLine(string) (telegraf.Metric, error) { return nil, errSentinel }
func (errorParser) SetDefaultTags(map[string]string)          {}

func TestSafeParserPassesThrough(t *testing.T) {
	if newSafeParser(nil) != nil {
		t.Fatal("nil parser should stay nil")
	}
	inner := newSafeParser(panickingParser{})
	if newSafeParser(inner) != inner {
		t.Fatal("wrapping an already safe parser should be a no-op")
	}
	if _, err := newSafeParser(errorParser{}).Parse(nil); !errors.Is(err, errSentinel) {
		t.Fatalf("expected sentinel error to pass through, got %v", err)
	}
}

// newTestAccumulator builds a real agent accumulator that discards metrics.
func newTestAccumulator(m *MQTTConsumerDB) telegraf.Accumulator {
	ch := make(chan telegraf.Metric, 100)
	go func() {
		for metric := range ch {
			metric.Accept()
		}
	}()
	return agent.NewAccumulator(&testMetricMaker{log: m.Log}, ch)
}

type testMetricMaker struct{ log telegraf.Logger }

func (*testMetricMaker) LogName() string                                   { return "inputs.mqtt_consumer_db" }
func (*testMetricMaker) MakeMetric(metric telegraf.Metric) telegraf.Metric { return metric }
func (t *testMetricMaker) Log() telegraf.Logger                            { return t.log }

// testLogger is a minimal telegraf.Logger that records nothing. It avoids
// pulling telegraf/testutil and its dependencies into go.mod.
type testLogger struct{}

func (*testLogger) Level() telegraf.LogLevel { return telegraf.Debug }
func (*testLogger) AddAttribute(string, any) {}
func (*testLogger) Errorf(string, ...any)    {}
func (*testLogger) Error(...any)             {}
func (*testLogger) Warnf(string, ...any)     {}
func (*testLogger) Warn(...any)              {}
func (*testLogger) Infof(string, ...any)     {}
func (*testLogger) Info(...any)              {}
func (*testLogger) Debugf(string, ...any)    {}
func (*testLogger) Debug(...any)             {}
func (*testLogger) Tracef(string, ...any)    {}
func (*testLogger) Trace(...any)             {}
