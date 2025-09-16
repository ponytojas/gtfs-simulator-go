package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gtfs-simulator/internal/config"
	"gtfs-simulator/internal/db"
	"gtfs-simulator/internal/metrics"
	"gtfs-simulator/internal/publisher"
	"gtfs-simulator/internal/sim"
)

func main() {
	// Load configuration from .env and environment
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Root context with cancellation on SIGINT/SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Resolve latest city database if CITY is set; connect to cluster's meta DB first (usually 'postgres')
	var sqlDB *sql.DB
	var currentDBName string
	{
		baseDSN := cfg.DatabaseURL
		// Ensure we connect to the 'postgres' database to read latest_successful_imports
		rootDSN, err := db.WithDBName(baseDSN, "postgres")
		if err != nil {
			log.Fatalf("invalid base DSN: %v", err)
		}
		metaDB, err := db.Open(rootDSN)
		if err != nil {
			log.Fatalf("db open (meta) error: %v", err)
		}
		defer metaDB.Close()
		if err := db.Ping(ctx, metaDB); err != nil {
			log.Fatalf("db ping (meta) error: %v", err)
		}
		finalDSN := baseDSN
		if cfg.City != "" {
			name, err := db.ResolveLatestImportDBName(ctx, metaDB, cfg.City)
			if err != nil {
				log.Fatalf("resolve latest import for city %q: %v", cfg.City, err)
			}
			currentDBName = name
			finalDSN, err = db.WithDBName(baseDSN, name)
			if err != nil {
				log.Fatalf("compose DSN: %v", err)
			}
			log.Printf("Using database %q for city %q", name, cfg.City)
		}
		sqlDB, err = db.Open(finalDSN)
		if err != nil {
			log.Fatalf("db open (city) error: %v", err)
		}
		// bind to outer scope close
		defer sqlDB.Close()
		if err := db.Ping(ctx, sqlDB); err != nil {
			log.Fatalf("db ping (city) error: %v", err)
		}
	}

	// Metrics setup
	var mcol *metrics.Collector
	var metricsSrvCancel context.CancelFunc
	if cfg.MetricsAddr != "" {
		mcol = metrics.NewCollector(cfg.SpeedMultiplier, cfg.PublishInterval, cfg.TripsRefreshInterval, cfg.PreloadHorizon)
		// Serve metrics
		mctx, mcancel := context.WithCancel(ctx)
		metricsSrvCancel = mcancel
		srv := mcol.Serve(cfg.MetricsAddr)
		go func() {
			<-mctx.Done()
			// Shutdown with timeout
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
	}

	// Initialize NATS publisher
    pub, err := publisher.NewNATSPublisher(cfg.NATSURL, cfg.LogNATSSubjects, wrapPublisherMetrics(mcol), cfg.NATSStreamName)
    if err != nil {
        log.Fatalf("nats error: %v", err)
    }
	defer pub.Close()

	// Fetch active trips for today and start simulation manager
	tz := cfg.Location
	mgr := startManagerForNow(ctx, sqlDB, pub, cfg, tz, mcol)
	// Start periodic trip refresher to launch new trips as they become active
	mgr.StartRefresher(ctx)

	// Start periodic city DB watcher (every 30 minutes) if CITY is set
	var done chan struct{}
	if cfg.City != "" {
		done = make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Minute)
			defer ticker.Stop()
			baseDSN := cfg.DatabaseURL
			for {
				select {
				case <-ctx.Done():
					close(done)
					return
				case <-ticker.C:
				}

				// 1) Ping current DB; if it fails, force re-resolve
				needSwitch := false
				if err := db.Ping(ctx, sqlDB); err != nil {
					log.Printf("db ping failed: %v — re-resolving city DB", err)
					if mcol != nil {
						mcol.DBSwitches.WithLabelValues("ping_failure").Inc()
					}
					needSwitch = true
				}

				// 2) Always re-resolve latest import, compare db_name
				// Use a short-lived meta connection
				rootDSN, _ := db.WithDBName(baseDSN, "postgres")
				metaDB, err := db.Open(rootDSN)
				if err != nil {
					log.Printf("meta db open error: %v", err)
					continue
				}
				if err := db.Ping(ctx, metaDB); err != nil {
					log.Printf("meta db ping error: %v", err)
					metaDB.Close()
					continue
				}
				newName, err := db.ResolveLatestImportDBName(ctx, metaDB, cfg.City)
				metaDB.Close()
				if err != nil {
					log.Printf("resolve latest import error: %v", err)
					continue
				}
				if newName != "" && newName != currentDBName {
					log.Printf("Detected updated DB for city %q: %q -> %q", cfg.City, currentDBName, newName)
					if mcol != nil {
						mcol.DBSwitches.WithLabelValues("update").Inc()
					}
					needSwitch = true
				}

				if !needSwitch {
					continue
				}

				// Compose DSN for target DB and try to connect
				targetName := currentDBName
				if newName != "" {
					targetName = newName
				}
				newDSN, err := db.WithDBName(baseDSN, targetName)
				if err != nil {
					log.Printf("compose DSN error: %v", err)
					continue
				}
				newDB, err := db.Open(newDSN)
				if err != nil {
					log.Printf("open new DB error: %v", err)
					continue
				}
				if err := db.Ping(ctx, newDB); err != nil {
					log.Printf("ping new DB error: %v", err)
					newDB.Close()
					continue
				}

				// Stop current manager and switch
				mgr.Stop()
				sqlDB.Close()
				sqlDB = newDB
				currentDBName = targetName
				log.Printf("Switched to DB %q for city %q", currentDBName, cfg.City)

				// Start a fresh manager with new DB and current trips
				mgr = startManagerForNow(ctx, sqlDB, pub, cfg, tz, mcol)
				mgr.StartRefresher(ctx)
			}
		}()
	}

	// Block until context cancelled
	<-ctx.Done()
	// Allow graceful shutdown
	mgr.Stop()
	if done != nil {
		<-done
	}
	if metricsSrvCancel != nil {
		metricsSrvCancel()
	}
	log.Println("shutdown complete")
	_ = os.Stderr
}

func startManagerForNow(ctx context.Context, sqlDB *sql.DB, pub *publisher.NATSPublisher, cfg *config.Config, tz *time.Location, mcol *metrics.Collector) *sim.Manager {
	now := time.Now().In(tz)
	activeTrips, err := db.FetchActiveTrips(ctx, sqlDB, now)
	if err != nil {
		log.Fatalf("fetch active trips error: %v", err)
	}
	if len(activeTrips) == 0 {
		log.Printf("no active trips for %s", now.Format("2006-01-02"))
	}
	mgr := sim.NewManager(sqlDB, pub, cfg.PublishInterval, cfg.SpeedMultiplier, tz, cfg.TripsRefreshInterval, cfg.PreloadHorizon, mcol)
	mgr.Start(ctx, activeTrips)
	return mgr
}

// wrapPublisherMetrics adapts our Collector to the PublisherMetrics interface.
func wrapPublisherMetrics(c *metrics.Collector) publisher.PublisherMetrics {
	if c == nil {
		return nil
	}
	return &pubMetrics{c: c}
}

type pubMetrics struct{ c *metrics.Collector }

func (p *pubMetrics) NATSPublishedInc()              { p.c.NATSPublished.Inc() }
func (p *pubMetrics) NATSPublishErrInc()             { p.c.NATSPublishErrs.Inc() }
func (p *pubMetrics) PublishObserve(d time.Duration) { p.c.PublishDuration.Observe(d.Seconds()) }
func (p *pubMetrics) NATSSetConnected(b bool) {
	if b {
		p.c.NATSConnected.Set(1)
	} else {
		p.c.NATSConnected.Set(0)
	}
}
