package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"gtfs-simulator/internal/gtfs"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

func Ping(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// FetchActiveTrips returns trips that are active on the given date
// and their start/end absolute times based on stop_times.
func FetchActiveTrips(ctx context.Context, db *sql.DB, now time.Time) ([]gtfs.ActiveTrip, error) {
	serviceIDs, err := fetchActiveServiceIDs(ctx, db, now)
	if err != nil {
		return nil, err
	}
	if len(serviceIDs) == 0 {
		return nil, nil
	}

	// Fetch trips for service IDs
	q := `SELECT trip_id, route_id, COALESCE(shape_id, '') FROM trips WHERE service_id = ANY($1)`
	rows, err := db.QueryContext(ctx, q, pqArray(serviceIDs))
	if err != nil {
		return nil, fmt.Errorf("query trips: %w", err)
	}
	defer rows.Close()

	var trips []gtfs.ActiveTrip
	for rows.Next() {
		var t gtfs.ActiveTrip
		if err := rows.Scan(&t.TripID, &t.RouteID, &t.ShapeID); err != nil {
			return nil, err
		}
		// Derive start/end times from stop_times
		st, err := fetchTripStartEnd(ctx, db, t.TripID, now)
		if err != nil {
			// Skip trips without stop_times
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		t.StartTime = st[0]
		t.EndTime = st[1]
		t.ServiceID = "" // not used downstream
		trips = append(trips, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return trips, nil
}

func fetchActiveServiceIDs(ctx context.Context, db *sql.DB, now time.Time) ([]string, error) {
	date := now.Format("2006-01-02")
	dow := int(now.Weekday()) // 0=Sunday

	// calendar has booleans (0/1). calendar_dates has exception_type (1 add, 2 remove)
	// Assume these columns are of standard types created by postgis-gtfs-importer.
	q := `
WITH base AS (
  SELECT service_id
  FROM calendar
  WHERE start_date <= $1::date AND end_date >= $1::date
    AND (
      ($2 = 0 AND (sunday::text IN ('1','t','true','available'))) OR
      ($2 = 1 AND (monday::text IN ('1','t','true','available'))) OR
      ($2 = 2 AND (tuesday::text IN ('1','t','true','available'))) OR
      ($2 = 3 AND (wednesday::text IN ('1','t','true','available'))) OR
      ($2 = 4 AND (thursday::text IN ('1','t','true','available'))) OR
      ($2 = 5 AND (friday::text IN ('1','t','true','available'))) OR
      ($2 = 6 AND (saturday::text IN ('1','t','true','available')))
    )
), add_exc AS (
  SELECT service_id FROM calendar_dates WHERE date = $1::date AND (exception_type::text IN ('1','added'))
), rm_exc AS (
  SELECT service_id FROM calendar_dates WHERE date = $1::date AND (exception_type::text IN ('2','removed'))
), merged AS (
  SELECT service_id FROM base
  UNION
  SELECT service_id FROM add_exc
)
SELECT DISTINCT service_id FROM merged
WHERE service_id NOT IN (SELECT service_id FROM rm_exc)
`

	rows, err := db.QueryContext(ctx, q, date, dow)
	if err != nil {
		return nil, fmt.Errorf("query active services: %w", err)
	}
	defer rows.Close()
	var svc []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		svc = append(svc, s)
	}
	return svc, rows.Err()
}

func fetchTripStartEnd(ctx context.Context, db *sql.DB, tripID string, now time.Time) ([2]time.Time, error) {
	// arrival_time and departure_time may be stored as text; get first/last
	q := `
SELECT COALESCE(MIN(departure_time)::text, MIN(arrival_time)::text) AS start_t,
       COALESCE(MAX(arrival_time)::text, MAX(departure_time)::text) AS end_t
FROM stop_times WHERE trip_id = $1`

	var startS, endS sql.NullString
	err := db.QueryRowContext(ctx, q, tripID).Scan(&startS, &endS)
	if err != nil {
		return [2]time.Time{}, err
	}
	if !startS.Valid || !endS.Valid {
		return [2]time.Time{}, sql.ErrNoRows
	}

	base := midnight(now)
	start := parseHHMMSSFlexible(startS.String, base)
	end := parseHHMMSSFlexible(endS.String, base)
	if end.Before(start) {
		// handle overnight wrap using 24+ hour times properly
		end = end.Add(24 * time.Hour)
	}
	return [2]time.Time{start, end}, nil
}

func FetchShapePoints(ctx context.Context, db *sql.DB, shapeID string) ([]gtfs.ShapePoint, error) {
	if shapeID == "" {
		return nil, nil
	}
	// Detect column layout: either shape_pt_lat/lon exist, or use PostGIS shape_pt_loc geography
	latlonExists, err := hasColumns(ctx, db, "public", "shapes", "shape_pt_lat", "shape_pt_lon")
	if err != nil {
		return nil, fmt.Errorf("introspect shapes columns: %w", err)
	}
	var q string
	if latlonExists["shape_pt_lat"] && latlonExists["shape_pt_lon"] {
		q = `SELECT shape_pt_lat, shape_pt_lon, shape_pt_sequence, COALESCE(shape_dist_traveled, 0)
             FROM shapes WHERE shape_id = $1 ORDER BY shape_pt_sequence`
	} else {
		// Fallback to geography point column shape_pt_loc
		locExists, err := hasColumns(ctx, db, "public", "shapes", "shape_pt_loc")
		if err != nil {
			return nil, fmt.Errorf("introspect shapes shape_pt_loc: %w", err)
		}
		if !locExists["shape_pt_loc"] {
			return nil, fmt.Errorf("shapes table missing expected columns (lat/lon or shape_pt_loc)")
		}
		q = `SELECT ST_Y(shape_pt_loc::geometry) AS lat,
                    ST_X(shape_pt_loc::geometry) AS lon,
                    shape_pt_sequence,
                    COALESCE(shape_dist_traveled, 0)
             FROM shapes WHERE shape_id = $1 ORDER BY shape_pt_sequence`
	}
	rows, err := db.QueryContext(ctx, q, shapeID)
	if err != nil {
		return nil, fmt.Errorf("query shapes: %w", err)
	}
	defer rows.Close()
	var pts []gtfs.ShapePoint
	for rows.Next() {
		var p gtfs.ShapePoint
		if err := rows.Scan(&p.Lat, &p.Lon, &p.Sequence, &p.DistTraveled); err != nil {
			return nil, err
		}
		// If GTFS stores shape_dist_traveled in km, convert. Typically it is in the same units as stop_times; we assume meters when > 1000.
		if p.DistTraveled > 0 && p.DistTraveled < 1000 {
			// leave as-is; ambiguous, keep input
		}
		pts = append(pts, p)
	}
	return pts, rows.Err()
}

func FetchStopTimes(ctx context.Context, db *sql.DB, tripID string) ([]gtfs.StopTime, error) {
	// Prefer stop_lat/stop_lon, but support PostGIS stop_loc geography as fallback
	latlonExists, err := hasColumns(ctx, db, "public", "stops", "stop_lat", "stop_lon")
	if err != nil {
		return nil, fmt.Errorf("introspect stops columns: %w", err)
	}
	var q string
	if latlonExists["stop_lat"] && latlonExists["stop_lon"] {
		q = `SELECT st.stop_sequence,
                    COALESCE(st.arrival_time::text,''),
                    COALESCE(st.departure_time::text,''),
                    COALESCE(st.shape_dist_traveled, 0),
                    st.stop_id,
                    COALESCE(s.stop_lat, 0),
                    COALESCE(s.stop_lon, 0)
             FROM stop_times st
             JOIN stops s ON s.stop_id = st.stop_id
             WHERE st.trip_id = $1
             ORDER BY st.stop_sequence`
	} else {
		locExists, err := hasColumns(ctx, db, "public", "stops", "stop_loc")
		if err != nil {
			return nil, fmt.Errorf("introspect stops stop_loc: %w", err)
		}
		if !locExists["stop_loc"] {
			return nil, fmt.Errorf("stops table missing expected columns (stop_lat/lon or stop_loc)")
		}
		q = `SELECT st.stop_sequence,
                    COALESCE(st.arrival_time::text,''),
                    COALESCE(st.departure_time::text,''),
                    COALESCE(st.shape_dist_traveled, 0),
                    st.stop_id,
                    COALESCE(ST_Y(s.stop_loc::geometry), 0),
                    COALESCE(ST_X(s.stop_loc::geometry), 0)
             FROM stop_times st
             JOIN stops s ON s.stop_id = st.stop_id
             WHERE st.trip_id = $1
             ORDER BY st.stop_sequence`
	}
	rows, err := db.QueryContext(ctx, q, tripID)
	if err != nil {
		return nil, fmt.Errorf("query stop_times: %w", err)
	}
	defer rows.Close()

	var sts []gtfs.StopTime
	for rows.Next() {
		var st gtfs.StopTime
		var arr, dep string
		if err := rows.Scan(&st.StopSequence, &arr, &dep, &st.ShapeDistTraveled, &st.StopID, &st.StopLat, &st.StopLon); err != nil {
			return nil, err
		}
		st.ArrivalSec = parseDaySeconds(arr)
		st.DepartureSec = parseDaySeconds(dep)
		sts = append(sts, st)
	}
	return sts, rows.Err()
}

func midnight(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

func parseHHMMSSFlexible(s string, base time.Time) time.Time {
	sec := parseDaySeconds(s)
	return base.Add(time.Duration(sec) * time.Second)
}

// parseDaySeconds parses HH:MM:SS possibly with hours >= 24.
func parseDaySeconds(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	parts := strings.Split(s, ":")
	if len(parts) < 2 {
		return 0
	}
	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	sec := 0
	if len(parts) > 2 {
		sec, _ = strconv.Atoi(parts[2])
	}
	total := h*3600 + m*60 + sec
	if total < 0 {
		total = 0
	}
	return total
}

func pqArray(a []string) any { return a }

// Haversine distance in meters
func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000.0
	toRad := func(d float64) float64 { return d * math.Pi / 180 }
	dLat := toRad(lat2 - lat1)
	dLon := toRad(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

// Build cumulative distances for shape points if not provided.
func CumDistances(pts []gtfs.ShapePoint) []float64 {
	n := len(pts)
	if n == 0 {
		return nil
	}
	cum := make([]float64, n)
	if pts[0].DistTraveled > 0 {
		// Assume provided distances are cumulative already. Normalize monotonicity.
		prev := 0.0
		for i := 0; i < n; i++ {
			d := pts[i].DistTraveled
			if d < prev {
				d = prev
			}
			cum[i] = d
			prev = d
		}
		return cum
	}
	// Compute via haversine
	sum := 0.0
	cum[0] = 0
	for i := 1; i < n; i++ {
		sum += haversine(pts[i-1].Lat, pts[i-1].Lon, pts[i].Lat, pts[i].Lon)
		cum[i] = sum
	}
	return cum
}

// Interpolate along shape by target distance; returns lat,lon and bearing.
func InterpolateShape(pts []gtfs.ShapePoint, cum []float64, dist float64) (lat, lon, bearing float64) {
	n := len(pts)
	if n == 0 {
		return 0, 0, 0
	}
	total := cum[n-1]
	if total == 0 {
		p := pts[0]
		return p.Lat, p.Lon, 0
	}
	if dist <= 0 {
		if n > 1 {
			return pts[0].Lat, pts[0].Lon, bearingDeg(pts[0], pts[1])
		}
		return pts[0].Lat, pts[0].Lon, 0
	}
	if dist >= total {
		if n > 1 {
			return pts[n-1].Lat, pts[n-1].Lon, bearingDeg(pts[n-2], pts[n-1])
		}
		return pts[n-1].Lat, pts[n-1].Lon, 0
	}
	// find segment
	i := 1
	for i < n && cum[i] < dist {
		i++
	}
	if i >= n {
		i = n - 1
	}
	d0 := cum[i-1]
	d1 := cum[i]
	p0 := pts[i-1]
	p1 := pts[i]
	if d1 == d0 {
		return p0.Lat, p0.Lon, bearingDeg(p0, p1)
	}
	frac := (dist - d0) / (d1 - d0)
	lat = p0.Lat + (p1.Lat-p0.Lat)*frac
	lon = p0.Lon + (p1.Lon-p0.Lon)*frac
	return lat, lon, bearingDeg(p0, p1)
}

func bearingDeg(a, b gtfs.ShapePoint) float64 {
	y := math.Sin((b.Lon-a.Lon)*math.Pi/180.0) * math.Cos(b.Lat*math.Pi/180.0)
	x := math.Cos(a.Lat*math.Pi/180.0)*math.Sin(b.Lat*math.Pi/180.0) - math.Sin(a.Lat*math.Pi/180.0)*math.Cos(b.Lat*math.Pi/180.0)*math.Cos((b.Lon-a.Lon)*math.Pi/180.0)
	brng := math.Atan2(y, x) * 180.0 / math.Pi
	if brng < 0 {
		brng += 360
	}
	return brng
}

// hasColumns returns a map of requested column names to existence for the given table.
func hasColumns(ctx context.Context, db *sql.DB, schema, table string, cols ...string) (map[string]bool, error) {
	res := make(map[string]bool, len(cols))
	if len(cols) == 0 {
		return res, nil
	}
	// Initialize to false
	for _, c := range cols {
		res[c] = false
	}
	q := `SELECT column_name FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2 AND column_name = ANY($3)`
	rows, err := db.QueryContext(ctx, q, schema, table, cols)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		res[name] = true
	}
	return res, rows.Err()
}

// NearestDistanceAlongShape finds the distance along the polyline closest to the given lat/lon.
// Uses an equirectangular approximation for projection to segments.
func NearestDistanceAlongShape(pts []gtfs.ShapePoint, cum []float64, lat, lon float64) float64 {
	n := len(pts)
	if n == 0 {
		return 0
	}
	if len(cum) != n {
		cum = CumDistances(pts)
	}
	// Precompute radians and reference
	// Use lat0 as the stop's latitude for scale; lon scale ~ cos(lat0)
	lat0 := lat * math.Pi / 180
	cosLat0 := math.Cos(lat0)
	toXY := func(p gtfs.ShapePoint) (x, y float64) {
		y = (p.Lat - lat) * math.Pi / 180 * 6371000.0
		x = (p.Lon - lon) * math.Pi / 180 * 6371000.0 * cosLat0
		return
	}
	bestDist2 := math.MaxFloat64
	bestAlong := 0.0
	// Iterate segments
	pPrev := pts[0]
	x0, y0 := toXY(pPrev)
	for i := 1; i < n; i++ {
		p1 := pts[i]
		x1, y1 := toXY(p1)
		dx := x1 - x0
		dy := y1 - y0
		segLen2 := dx*dx + dy*dy
		t := 0.0
		if segLen2 > 0 {
			t = -(x0*dx + y0*dy) / segLen2 // projection of origin onto segment
			if t < 0 {
				t = 0
			} else if t > 1 {
				t = 1
			}
		}
		px := x0 + t*dx
		py := y0 + t*dy
		d2 := px*px + py*py
		if d2 < bestDist2 {
			bestDist2 = d2
			// along distance accumulative
			bestAlong = cum[i-1] + t*(cum[i]-cum[i-1])
		}
		x0, y0 = x1, y1
	}
	return bestAlong
}
