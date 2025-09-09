package db

import (
	"fmt"
	"net/url"
	"strings"
)

// WithDBName returns a DSN identical to the input but with the database path replaced.
// Supports postgres:// and postgresql:// schemes.
func WithDBName(dsn, database string) (string, error) {
	if dsn == "" {
		return "", fmt.Errorf("empty DSN")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		// allow missing scheme by prefixing postgres://
		if !strings.Contains(dsn, "://") {
			dsn = "postgres://" + dsn
			u, err = url.Parse(dsn)
			if err != nil {
				return "", err
			}
		}
	}
	if !strings.HasPrefix(database, "/") {
		u.Path = "/" + database
	} else {
		u.Path = database
	}
	return u.String(), nil
}
