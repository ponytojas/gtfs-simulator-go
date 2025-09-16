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
    js          nats.JetStreamContext
    streamName  string
    logSubjects bool
    metrics     PublisherMetrics
}

type PublisherMetrics interface {
    NATSPublishedInc()
    NATSPublishErrInc()
    PublishObserve(d time.Duration)
    NATSSetConnected(connected bool)
}

func NewNATSPublisher(url string, logSubjects bool, m PublisherMetrics, streamName string) (*NATSPublisher, error) {
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

    // Initialize JetStream context
    js, err := nc.JetStream()
    if err != nil {
        nc.Close()
        return nil, err
    }

    // Ensure stream exists with subject pattern vehicles.>
    if streamName == "" {
        streamName = "VEHICLES"
    }
    // Try to fetch stream info; if not found, create it.
    if _, err := js.StreamInfo(streamName); err != nil {
        // Create stream
        _, addErr := js.AddStream(&nats.StreamConfig{
            Name:      streamName,
            Subjects:  []string{"vehicles.>"},
            Retention: nats.LimitsPolicy,
            Storage:   nats.FileStorage,
            Replicas:  1,
        })
        if addErr != nil {
            nc.Close()
            return nil, addErr
        }
        log.Printf("created JetStream stream %q with subjects [vehicles.>]", streamName)
    } else {
        // Optionally ensure the subject pattern is present by updating the stream if needed.
        // Fetch current config and update if vehicles.> missing.
        si, _ := js.StreamInfo(streamName)
        ensure := true
        if si != nil {
            for _, s := range si.Config.Subjects {
                if s == "vehicles.>" {
                    ensure = false
                    break
                }
            }
        }
        if ensure {
            // Merge existing subjects with vehicles.>
            subs := []string{"vehicles.>"}
            if si != nil {
                subs = append(subs, si.Config.Subjects...)
            }
            _, updErr := js.UpdateStream(&nats.StreamConfig{
                Name:      streamName,
                Subjects:  subs,
                Retention: nats.LimitsPolicy,
                Storage:   nats.FileStorage,
                Replicas:  1,
            })
            if updErr != nil {
                nc.Close()
                return nil, updErr
            }
            log.Printf("updated JetStream stream %q to include subject vehicles.>", streamName)
        }
    }

    return &NATSPublisher{nc: nc, js: js, streamName: streamName, logSubjects: logSubjects, metrics: m}, nil
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
	VehicleID string    `json:"vehicleId"`
	Timestamp time.Time `json:"timestamp"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Bearing   float64   `json:"bearing"`
	Progress  float64   `json:"progress"`
	SpeedMps  float64   `json:"speedMps"`
}

func (p *NATSPublisher) PublishPosition(routeID, tripID string, msg PositionMessage) error {
    subject := fmt.Sprintf("vehicles.%s.%s", subjectToken(routeID), subjectToken(tripID))
    b, err := json.Marshal(msg)
    if err != nil {
        return err
    }
    if p.logSubjects {
        log.Printf("nats publish subject=%s", subject)
    }
    start := time.Now()
    // Publish via JetStream to ensure persistence/ack
    if p.js == nil {
        return fmt.Errorf("jetstream not initialized")
    }
    _, err = p.js.Publish(subject, b)
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
