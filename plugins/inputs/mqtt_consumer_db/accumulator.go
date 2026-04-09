package mqtt_consumer_db

import (
	_ "embed"
	"fmt"
	"os"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

type CustomAccumulator struct {
	telegraf.Accumulator
	Debug    bool
	ServerID string
}

func (ca *CustomAccumulator) acc_debug(format string, args ...any) {
	if ca.Debug {
		fmt.Fprintf(os.Stderr, "[acc:%s] %s\n", ca.ServerID, fmt.Sprintf(format, args...))
	}
}

func (ca *CustomAccumulator) AddMetric(m telegraf.Metric) {
	ca.acc_debug("AddMetric: name=%q fields=%v tags=%v time=%v", m.Name(), m.Fields(), m.Tags(), m.Time().UTC())
	ca.Accumulator.AddMetric(m)
}

func (ca *CustomAccumulator) WithTracking(maxTracked int) telegraf.TrackingAccumulator {
	ca.acc_debug("WithTracking called maxTracked=%d", maxTracked)
	return &CustomTrackingAccumulator{
		Accumulator: ca,
		delivered:   make(chan telegraf.DeliveryInfo, maxTracked),
	}
}

type CustomTrackingAccumulator struct {
	telegraf.Accumulator
	delivered chan telegraf.DeliveryInfo
}

func (a *CustomTrackingAccumulator) AddTrackingMetric(m telegraf.Metric) telegraf.TrackingID {
	if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
		ca.acc_debug("AddTrackingMetric: name=%q fields=%v tags=%v", m.Name(), m.Fields(), m.Tags())
	}
	dm, id := metric.WithTracking(m, a.onDelivery)
	a.AddMetric(dm)
	return id
}

func (a *CustomTrackingAccumulator) AddTrackingMetricGroup(group []telegraf.Metric) telegraf.TrackingID {
	if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
		ca.acc_debug("AddTrackingMetricGroup: %d metric(s) in group", len(group))
	}
	// Set Measurement-Name to the second part of the topic
	for _, m := range group {
		topic, ok := m.GetTag("topic")
		if ok {
			s := strings.Split(topic, "/")
			len_s := len(s)

			if len_s > 0 {
				m.AddTag("organization", s[0])

				// Rename "value" field to the last topic segment
				fieldName := s[len_s-1]
				if val, ok := m.GetField("value"); ok {
					m.RemoveField("value")
					m.AddField(fieldName, val)
				}

				if len_s > 1 {
					product := s[1]

					version := ""
					product_version := product
					if len_s > 4 {
						version = s[3]
						product_version = product + "_" + version
					}

					m.SetName(product_version)

					m.AddTag("product", product)
					m.AddTag("version", version)

					if len_s > 2 {
						m.AddTag("serialnumber", s[2])

						m.AddTag("domain", s[len_s-1])

						if len_s > 4 {
							m.AddTag("root", strings.Join(s[:len_s-2], "/"))
						} else if len_s > 3 {
							m.AddTag("root", strings.Join(s[:len_s-1], "/"))
						}
					}
				}
			}

			/*if len_s > 0 {
				m.AddTag("organization", s[0])

				if len_s > 1 {
					product := s[1]

					m.SetName(product)

					// extract postfix
					version := "1"
					parts := strings.Split(product, "_v")
					parts_len := len(parts)
					if parts_len > 1 {
						product = strings.Join(parts[:parts_len-1], "_v")
						version = parts[parts_len-1]
					}

					m.AddTag("product", product)
					m.AddTag("version", version)
					s[1] = product
					m.AddTag("topic", strings.Join(s, "/"))

					if len_s > 2 {
						m.AddTag("serialnumber", s[2])
						m.AddTag("root", strings.Join(s[:len_s-1], "/"))

						if len_s > 3 {
							m.AddTag("domain", s[len_s-1])
						}
					}
				}
			}*/
		}
	}

	if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
		for i, m := range group {
			ca.acc_debug("  group[%d]: name=%q fields=%v tags=%v", i, m.Name(), m.Fields(), m.Tags())
		}
	}

	db, id := metric.WithGroupTracking(group, a.onDelivery)
	for _, m := range db {
		a.AddMetric(m)
	}
	if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
		ca.acc_debug("group tracking id=%v, %d metric(s) forwarded", id, len(db))
	}
	return id
}

func (a *CustomTrackingAccumulator) Delivered() <-chan telegraf.DeliveryInfo {
	return a.delivered
}

func (a *CustomTrackingAccumulator) onDelivery(info telegraf.DeliveryInfo) {
	if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
		ca.acc_debug("onDelivery: id=%v delivered=%v", info.ID(), info.Delivered())
	}
	select {
	case a.delivered <- info:
	default:
		if ca, ok := a.Accumulator.(*CustomAccumulator); ok {
			ca.acc_debug("onDelivery: channel FULL for id=%v (cap=%d)", info.ID(), cap(a.delivered))
		}
		panic("channel is full")
	}
}
