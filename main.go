package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	influxser "github.com/influxdata/telegraf/plugins/serializers/influx"

	mqttdb "mqtt_consumer_db/plugins/inputs/mqtt_consumer_db"
)

var pollInterval = flag.Duration("poll_interval", 1*time.Second, "how often to send metrics")
var pollIntervalDisabled = flag.Bool("poll_interval_disabled", false, "set to true to disable polling")
var configFile = flag.String("config", "", "path to the config file for this plugin")

// --- TOML config structure ---

type pluginConfig struct {
	Inputs struct {
		MQTTConsumerDB []toml.Primitive `toml:"mqtt_consumer_db"`
	} `toml:"inputs"`
}

// --- stderrLogger implements telegraf.Logger ---

type stderrLogger struct{ prefix string }

func (l *stderrLogger) Level() telegraf.LogLevel         { return telegraf.Debug }
func (l *stderrLogger) AddAttribute(string, interface{}) {}
func (l *stderrLogger) Errorf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[E] [%s] %s\n", l.prefix, fmt.Sprintf(f, a...))
}
func (l *stderrLogger) Error(a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[E] [%s] %s\n", l.prefix, fmt.Sprint(a...))
}
func (l *stderrLogger) Warnf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[W] [%s] %s\n", l.prefix, fmt.Sprintf(f, a...))
}
func (l *stderrLogger) Warn(a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[W] [%s] %s\n", l.prefix, fmt.Sprint(a...))
}
func (l *stderrLogger) Infof(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[I] [%s] %s\n", l.prefix, fmt.Sprintf(f, a...))
}
func (l *stderrLogger) Info(a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[I] [%s] %s\n", l.prefix, fmt.Sprint(a...))
}
func (l *stderrLogger) Debugf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[D] [%s] %s\n", l.prefix, fmt.Sprintf(f, a...))
}
func (l *stderrLogger) Debug(a ...interface{}) {
	fmt.Fprintf(os.Stderr, "[D] [%s] %s\n", l.prefix, fmt.Sprint(a...))
}
func (l *stderrLogger) Tracef(f string, a ...interface{}) {}
func (l *stderrLogger) Trace(a ...interface{})            {}

// --- stdoutAccumulator implements telegraf.Accumulator ---

type stdoutAccumulator struct {
	mu         sync.Mutex
	serializer *influxser.Serializer
}

func newStdoutAccumulator() (*stdoutAccumulator, error) {
	s := &influxser.Serializer{}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &stdoutAccumulator{serializer: s}, nil
}

func (a *stdoutAccumulator) addMetricFields(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	now := time.Now()
	if len(t) > 0 {
		now = t[0]
	}
	m := metric.New(measurement, tags, fields, now)
	a.AddMetric(m)
}

func (a *stdoutAccumulator) AddFields(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	a.addMetricFields(measurement, fields, tags, t...)
}
func (a *stdoutAccumulator) AddGauge(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	a.addMetricFields(measurement, fields, tags, t...)
}
func (a *stdoutAccumulator) AddCounter(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	a.addMetricFields(measurement, fields, tags, t...)
}
func (a *stdoutAccumulator) AddSummary(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	a.addMetricFields(measurement, fields, tags, t...)
}
func (a *stdoutAccumulator) AddHistogram(measurement string, fields map[string]interface{}, tags map[string]string, t ...time.Time) {
	a.addMetricFields(measurement, fields, tags, t...)
}

func (a *stdoutAccumulator) AddMetric(m telegraf.Metric) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, err := a.serializer.Serialize(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "serialization error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "[stdout] writing metric: %s", string(b))
	_, writeErr := os.Stdout.Write(b)
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "[stdout] write error: %v\n", writeErr)
	}
	m.Accept()
}

func (a *stdoutAccumulator) SetPrecision(time.Duration) {}

func (a *stdoutAccumulator) AddError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func (a *stdoutAccumulator) WithTracking(maxTracked int) telegraf.TrackingAccumulator {
	// CustomAccumulator in the plugin overrides this; should never reach here
	panic("WithTracking called on base stdoutAccumulator")
}

// --- main ---

func main() {
	flag.Parse()
	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "error: --config flag is required")
		os.Exit(1)
	}
	if *pollIntervalDisabled {
		*pollInterval = 0
	}

	// Read and expand environment variables in config
	raw, err := os.ReadFile(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading config: %v\n", err)
		os.Exit(1)
	}

	var conf pluginConfig
	md, err := toml.Decode(os.ExpandEnv(string(raw)), &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing config: %v\n", err)
		os.Exit(1)
	}

	primitives := conf.Inputs.MQTTConsumerDB
	if len(primitives) == 0 {
		fmt.Fprintln(os.Stderr, "no [[inputs.mqtt_consumer_db]] sections found in config")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "found %d plugin instance(s)\n", len(primitives))

	// Shared accumulator for line-protocol output
	acc, err := newStdoutAccumulator()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating accumulator: %v\n", err)
		os.Exit(1)
	}

	// Create and init all instances
	var plugins []*mqttdb.MQTTConsumerDB
	for i, prim := range primitives {
		p := mqttdb.New()
		if err := md.PrimitiveDecode(prim, p); err != nil {
			fmt.Fprintf(os.Stderr, "error decoding instance %d: %v\n", i, err)
			os.Exit(1)
		}
		p.Log = &stderrLogger{prefix: "mqtt_consumer_db:" + p.ServerID}
		fmt.Fprintf(os.Stderr, "instance %d: server_id=%q data_format=%q\n", i, p.ServerID, p.DataFormat)

		if err := p.Init(); err != nil {
			fmt.Fprintf(os.Stderr, "error initializing instance %d (server_id=%q): %v\n", i, p.ServerID, err)
			os.Exit(1)
		}
		plugins = append(plugins, p)
	}

	// Start all instances
	for i, p := range plugins {
		if err := p.Start(acc); err != nil {
			fmt.Fprintf(os.Stderr, "error starting instance %d (server_id=%q): %v\n", i, p.ServerID, err)
			for j := i - 1; j >= 0; j-- {
				plugins[j].Stop()
			}
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "started instance %d: server_id=%q\n", i, p.ServerID)
	}

	// Context for shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Watch stdin for close (execd protocol)
	go func() {
		b := make([]byte, 1)
		for {
			if _, err := os.Stdin.Read(b); err != nil {
				cancel()
				return
			}
		}
	}()

	// Periodic gather (if enabled)
	if *pollInterval > 0 {
		go func() {
			ticker := time.NewTicker(*pollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for _, p := range plugins {
						if err := p.Gather(acc); err != nil {
							fmt.Fprintf(os.Stderr, "gather error: %v\n", err)
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Wait for shutdown
	select {
	case sig := <-sigCh:
		fmt.Fprintf(os.Stderr, "received signal %v, shutting down...\n", sig)
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "stdin closed, shutting down...")
	}

	// Stop all instances
	for _, p := range plugins {
		p.Stop()
	}
	fmt.Fprintln(os.Stderr, "all instances stopped")
}
