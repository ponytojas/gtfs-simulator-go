package metrics

import (
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Collector struct {
	reg *prometheus.Registry

	ActiveTrips    prometheus.Gauge
	ScheduledTrips prometheus.Gauge

	TripsStarted   prometheus.Counter
	TripsFinished  prometheus.Counter
	TripsScheduled prometheus.Counter

	NATSPublished   prometheus.Counter
	NATSPublishErrs prometheus.Counter
	NATSConnected   prometheus.Gauge

	DBSwitches *prometheus.CounterVec // reason label: update|ping_failure

	TickDuration    prometheus.Histogram
	PublishDuration prometheus.Histogram

	SpeedMultiplier prometheus.Gauge
	PublishInterval prometheus.Gauge // seconds
	RefreshInterval prometheus.Gauge // seconds
	PreloadMinutes  prometheus.Gauge
}

func NewCollector(speedMultiplier float64, publishInterval, refreshInterval time.Duration, preloadHorizon time.Duration) *Collector {
	reg := prometheus.NewRegistry()

	c := &Collector{
		reg: reg,
		ActiveTrips: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_active_trips",
			Help: "Number of currently running trip goroutines.",
		}),
		ScheduledTrips: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_scheduled_trips",
			Help: "Number of trips scheduled to start soon.",
		}),
		TripsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "simulator_trips_started_total",
			Help: "Total trips started.",
		}),
		TripsFinished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "simulator_trips_finished_total",
			Help: "Total trips finished.",
		}),
		TripsScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "simulator_trips_scheduled_total",
			Help: "Total trips scheduled for future start.",
		}),
		NATSPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "simulator_nats_published_total",
			Help: "Total NATS messages published.",
		}),
		NATSPublishErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "simulator_nats_publish_errors_total",
			Help: "Total NATS publish errors.",
		}),
		NATSConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_nats_connected",
			Help: "1 if NATS connection is established, 0 otherwise.",
		}),
		DBSwitches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "simulator_db_switches_total",
			Help: "Number of database switches.",
		}, []string{"reason"}),
		TickDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "simulator_tick_duration_seconds",
			Help:    "Duration of simulation tick computations.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
		PublishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "simulator_publish_duration_seconds",
			Help:    "Duration to marshal and publish a NATS message.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
		}),
		SpeedMultiplier: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_speed_multiplier",
			Help: "Current speed multiplier.",
		}),
		PublishInterval: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_publish_interval_seconds",
			Help: "Publish interval in seconds.",
		}),
		RefreshInterval: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_refresh_interval_seconds",
			Help: "Trips refresh interval in seconds.",
		}),
		PreloadMinutes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_preload_horizon_minutes",
			Help: "Preload horizon in minutes.",
		}),
	}

	// Register
	reg.MustRegister(
		c.ActiveTrips, c.ScheduledTrips,
		c.TripsStarted, c.TripsFinished, c.TripsScheduled,
		c.NATSPublished, c.NATSPublishErrs, c.NATSConnected,
		c.DBSwitches, c.TickDuration, c.PublishDuration,
		c.SpeedMultiplier, c.PublishInterval, c.RefreshInterval, c.PreloadMinutes,
	)

	// Set static/dynamic gauges
	c.SpeedMultiplier.Set(speedMultiplier)
	c.PublishInterval.Set(publishInterval.Seconds())
	c.RefreshInterval.Set(refreshInterval.Seconds())
	c.PreloadMinutes.Set(preloadHorizon.Minutes())

	return c
}

func (c *Collector) Handler() http.Handler { return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{}) }

// Serve starts an HTTP server exposing /metrics on the given address.
func (c *Collector) Serve(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", c.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
	log.Printf("metrics listening on %s", addr)
	return srv
}
