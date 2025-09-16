package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
    DatabaseURL          string
    NATSURL              string
    NATSStreamName       string
    PublishInterval      time.Duration
    TripsRefreshInterval time.Duration
    PreloadHorizon       time.Duration
    SpeedMultiplier      float64
    Location             *time.Location
	City                 string
	LogNATSSubjects      bool
	MetricsAddr          string
}

func Load() (*Config, error) {
	// Load .env into environment (ignore if missing)
	_ = godotenv.Load()

	cfg := &Config{}

	// Database URL (cluster DSN): prefer DATABASE_URL / PG_DSN, else build from PG* vars
	dsn := firstNonEmpty(
		os.Getenv("DATABASE_URL"),
		os.Getenv("PG_DSN"),
	)
	if dsn == "" {
		host := getenvDefault("PGHOST", "127.0.0.1")
		port := getenvDefault("PGPORT", "5432")
		user := getenvDefault("PGUSER", "postgres")
		pass := os.Getenv("PGPASSWORD")
		db := os.Getenv("PGDATABASE")
		// If CITY is provided, default base DB to 'postgres' when PGDATABASE is not set.
		if db == "" && os.Getenv("CITY") != "" {
			db = "postgres"
		}
		if db == "" {
			return nil, errors.New("PGDATABASE or DATABASE_URL must be set (set PGDATABASE=postgres when using CITY)")
		}
		sslmode := getenvDefault("PGSSLMODE", "disable")
		if pass != "" {
			cfg.DatabaseURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", urlEscape(user), urlEscape(pass), host, port, db, sslmode)
		} else {
			cfg.DatabaseURL = fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=%s", urlEscape(user), host, port, db, sslmode)
		}
	} else {
		cfg.DatabaseURL = dsn
	}

	cfg.NATSURL = getenvDefault("NATS_URL", "nats://127.0.0.1:4222")

	// JetStream stream name for vehicle subjects
	cfg.NATSStreamName = getenvDefault("NATS_STREAM_NAME", "VEHICLES")

	// Publish interval
	if v := os.Getenv("PUBLISH_INTERVAL_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("invalid PUBLISH_INTERVAL_MS: %q", v)
		}
		cfg.PublishInterval = time.Duration(ms) * time.Millisecond
	} else {
		cfg.PublishInterval = time.Second
	}

	// Speed multiplier
	if v := os.Getenv("SPEED_MULTIPLIER"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			return nil, fmt.Errorf("invalid SPEED_MULTIPLIER: %q", v)
		}
		cfg.SpeedMultiplier = f
	} else {
		cfg.SpeedMultiplier = 1.0
	}

	// Trips refresh interval (seconds)
	if v := os.Getenv("TRIPS_REFRESH_INTERVAL_SEC"); v != "" {
		sec, err := strconv.Atoi(v)
		if err != nil || sec <= 0 {
			return nil, fmt.Errorf("invalid TRIPS_REFRESH_INTERVAL_SEC: %q", v)
		}
		cfg.TripsRefreshInterval = time.Duration(sec) * time.Second
	} else {
		cfg.TripsRefreshInterval = 60 * time.Second
	}

	// Preload horizon (minutes)
	if v := os.Getenv("TRIPS_PRELOAD_MINUTES"); v != "" {
		min, err := strconv.Atoi(v)
		if err != nil || min < 0 {
			return nil, fmt.Errorf("invalid TRIPS_PRELOAD_MINUTES: %q", v)
		}
		cfg.PreloadHorizon = time.Duration(min) * time.Minute
	} else {
		cfg.PreloadHorizon = 30 * time.Minute
	}

	// Debug logging for NATS publish subjects
	if v := os.Getenv("LOG_NATS_SUBJECTS"); v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "t", "yes", "y", "on":
			cfg.LogNATSSubjects = true
		default:
			cfg.LogNATSSubjects = false
		}
	}

	// Metrics listen address (e.g., ":9102"). Empty disables the metrics server.
	cfg.MetricsAddr = os.Getenv("METRICS_ADDR")

	// Time zone
	tzName := getenvDefault("TZ", "")
	if tzName == "" {
		cfg.Location = time.Local
	} else {
		loc, err := time.LoadLocation(tzName)
		if err != nil {
			return nil, fmt.Errorf("invalid TZ: %v", err)
		}
		cfg.Location = loc
	}

	// City name for dynamic DB resolution
	cfg.City = firstNonEmpty(os.Getenv("CITY"), os.Getenv("CITY_NAME"))

	return cfg, nil
}

func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func urlEscape(s string) string {
	// Minimal escape for DSN user/pass with special chars
	r := strings.NewReplacer("@", "%40", ":", "%3A", "/", "%2F", "?", "%3F", "#", "%23")
	return r.Replace(s)
}
