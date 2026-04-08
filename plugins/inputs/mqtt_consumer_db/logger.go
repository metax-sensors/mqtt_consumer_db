package mqtt_consumer_db

import (
	"github.com/influxdata/telegraf"
)

// levelFilterLogger forwards all logs, but downgrades info messages to debug.
// This avoids misleading info-lines appearing as errors in execd stderr output.
type levelFilterLogger struct {
	telegraf.Logger
}

func (l levelFilterLogger) Infof(format string, args ...interface{}) {
	l.Logger.Debugf(format, args...)
}

func (l levelFilterLogger) Info(args ...interface{}) {
	l.Logger.Debug(args...)
}

type mqttLogger struct {
	telegraf.Logger
}

func (l mqttLogger) Printf(fmt string, args ...interface{}) {
	l.Logger.Debugf(fmt, args...)
}

func (l mqttLogger) Println(args ...interface{}) {
	l.Logger.Debug(args...)
}
