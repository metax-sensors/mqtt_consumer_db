package mqtt_consumer_db

import (
	_ "embed"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

type CustomAccumulator struct {
	telegraf.Accumulator
}

func (ca *CustomAccumulator) WithTracking(maxTracked int) telegraf.TrackingAccumulator {
	return &CustomTrackingAccumulator{
		Accumulator: ca,
		delivered: make(chan telegraf.DeliveryInfo, maxTracked),
	}
}

type CustomTrackingAccumulator struct {
	telegraf.Accumulator
	delivered chan telegraf.DeliveryInfo
}

func (a *CustomTrackingAccumulator) AddTrackingMetric(m telegraf.Metric) telegraf.TrackingID {
	dm, id := metric.WithTracking(m, a.onDelivery)
	a.AddMetric(dm)
	return id
}

func (a *CustomTrackingAccumulator) AddTrackingMetricGroup(group []telegraf.Metric) telegraf.TrackingID {
	// Set Measurement-Name to the second part of the topic
	for _, m := range group {
		topic,ok := m.GetTag("topic")
		if ok {
			s := strings.Split(topic, "/")
			len_s := len(s)

			if len_s > 0 {
				m.AddTag("organization", s[0])

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
					//m.AddTag("topic", strings.Join(s, "/"))

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

	db, id := metric.WithGroupTracking(group, a.onDelivery)
	for _, m := range db {
		a.AddMetric(m)
	}
	return id
}

func (a *CustomTrackingAccumulator) Delivered() <-chan telegraf.DeliveryInfo {
	return a.delivered
}

func (a *CustomTrackingAccumulator) onDelivery(info telegraf.DeliveryInfo) {
	select {
	case a.delivered <- info:
	default:
		// This is a programming error in the input.  More items were sent for
		// tracking than space requested.
		panic("channel is full")
	}
}
