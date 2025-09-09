package publisher

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NATSPublisher struct {
	nc          *nats.Conn
	logSubjects bool
	metrics     PublisherMetrics
}

type PublisherMetrics interface {
	NATSPublishedInc()
	NATSPublishErrInc()
	PublishObserve(d time.Duration)
	NATSSetConnected(connected bool)
}

func NewNATSPublisher(url string, logSubjects bool, m PublisherMetrics) (*NATSPublisher, error) {
	nc, err := nats.Connect(url,
		nats.Name("gtfs-simulator"),
		nats.DisconnectHandler(func(_ *nats.Conn) {
			if m != nil {
				m.NATSSetConnected(false)
			}
			log.Printf("nats disconnected")
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			if m != nil {
				m.NATSSetConnected(true)
			}
			log.Printf("nats reconnected")
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			if m != nil {
				m.NATSSetConnected(false)
			}
			log.Printf("nats closed")
		}),
	)
	if err != nil {
		return nil, err
	}
	if m != nil {
		m.NATSSetConnected(true)
	}
	return &NATSPublisher{nc: nc, logSubjects: logSubjects, metrics: m}, nil
}

func (p *NATSPublisher) Close() {
	if p.nc != nil {
		p.nc.Drain()
		p.nc.Close()
	}
}

type PositionMessage struct {
	TripID    string    `json:"tripId"`
	RouteID   string    `json:"routeId"`
	Timestamp time.Time `json:"timestamp"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Bearing   float64   `json:"bearing"`
	Progress  float64   `json:"progress"`
	SpeedMps  float64   `json:"speedMps"`
}

func (p *NATSPublisher) PublishPosition(routeID, tripID string, msg PositionMessage) error {
	subject := fmt.Sprintf("%s.%s", subjectToken(routeID), subjectToken(tripID))
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if p.logSubjects {
		log.Printf("nats publish subject=%s", subject)
	}
	start := time.Now()
	err = p.nc.Publish(subject, b)
	if p.metrics != nil {
		p.metrics.PublishObserve(time.Since(start))
		if err != nil {
			p.metrics.NATSPublishErrInc()
		} else {
			p.metrics.NATSPublishedInc()
		}
	}
	return err
}

func subjectToken(s string) string {
	s = strings.TrimSpace(s)
	// NATS token cannot contain spaces, '>', '*', or trailing '.'
	repl := strings.NewReplacer(" ", "_", ".", "_", ">", "_", "*", "_", "/", "_", "\t", "_")
	s = repl.Replace(s)
	if s == "" {
		s = "_"
	}
	return s
}
