package mqtt_consumer_db

import (
	"fmt"

	"github.com/influxdata/telegraf"
)

// maxPayloadInError limits how much of an offending payload ends up in logs.
const maxPayloadInError = 256

// safeParser wraps a telegraf.Parser and converts panics raised while parsing
// into ordinary errors. The wrapped parser is invoked from paho's message
// callback, which has no panic recovery, so without this guard a single
// malformed message terminates the plugin process and every message that
// arrives until execd has restarted it is lost.
type safeParser struct {
	telegraf.Parser
}

func newSafeParser(p telegraf.Parser) telegraf.Parser {
	if p == nil {
		return nil
	}
	if _, ok := p.(*safeParser); ok {
		return p
	}
	return &safeParser{Parser: p}
}

func (p *safeParser) Parse(buf []byte) (metrics []telegraf.Metric, err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics = nil
			err = fmt.Errorf("parser panicked: %v (payload: %q)", r, truncate(buf))
		}
	}()
	return p.Parser.Parse(buf)
}

func (p *safeParser) ParseLine(line string) (m telegraf.Metric, err error) {
	defer func() {
		if r := recover(); r != nil {
			m = nil
			err = fmt.Errorf("parser panicked: %v (line: %q)", r, truncate([]byte(line)))
		}
	}()
	return p.Parser.ParseLine(line)
}

func truncate(b []byte) string {
	if len(b) > maxPayloadInError {
		return string(b[:maxPayloadInError]) + "..."
	}
	return string(b)
}
