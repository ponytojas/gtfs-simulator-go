package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ResolveLatestImportDBName returns the db_name with the most recent imported_at
// from public.latest_successful_imports where db_name ILIKE '%city%'.
func ResolveLatestImportDBName(ctx context.Context, meta *sql.DB, city string) (string, error) {
	city = strings.TrimSpace(city)
	if city == "" {
		return "", fmt.Errorf("city is required")
	}
	// Fully qualified to the public schema (assumes we are connected to the 'postgres' database)
	q := `
SELECT db_name
FROM public.latest_successful_imports
WHERE db_name ILIKE '%' || $1 || '%'
ORDER BY imported_at DESC
LIMIT 1`
	var dbName sql.NullString
	if err := meta.QueryRowContext(ctx, q, city).Scan(&dbName); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no database found for city like %q", city)
		}
		return "", err
	}
	if !dbName.Valid || dbName.String == "" {
		return "", fmt.Errorf("empty db_name for city like %q", city)
	}
	return dbName.String, nil
}
