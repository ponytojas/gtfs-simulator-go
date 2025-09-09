package sim

import (
	"context"
	"database/sql"
	"log"
	"math"
	"sync"
	"time"

	"gtfs-simulator/internal/db"
	"gtfs-simulator/internal/gtfs"
	mmetrics "gtfs-simulator/internal/metrics"
	"gtfs-simulator/internal/publisher"
)

type Manager struct {
	db              *sql.DB
	pub             *publisher.NATSPublisher
	publishInterval time.Duration
	speedMultiplier float64
	tz              *time.Location
	refreshInterval time.Duration
	preloadHorizon  time.Duration
	metrics         *mmetrics.Collector

	mu      sync.Mutex
	running map[string]context.CancelFunc // tripID -> cancel
	wg      sync.WaitGroup

	refreshCancel context.CancelFunc
	refreshWG     sync.WaitGroup

	scheduled   map[string]context.CancelFunc // tripID -> cancel (not yet started)
	scheduledWG sync.WaitGroup
}

func NewManager(dbConn *sql.DB, pub *publisher.NATSPublisher, publishInterval time.Duration, speedMultiplier float64, tz *time.Location, refreshInterval time.Duration, preloadHorizon time.Duration, metrics *mmetrics.Collector) *Manager {
	return &Manager{
		db:              dbConn,
		pub:             pub,
		publishInterval: publishInterval,
		speedMultiplier: speedMultiplier,
		tz:              tz,
		refreshInterval: refreshInterval,
		preloadHorizon:  preloadHorizon,
		metrics:         metrics,
		running:         make(map[string]context.CancelFunc),
		scheduled:       make(map[string]context.CancelFunc),
	}
}

func (m *Manager) Start(ctx context.Context, trips []gtfs.ActiveTrip) {
	now := time.Now().In(m.tz)
	for _, t := range trips {
		if now.Before(t.StartTime) || now.After(t.EndTime) {
			continue // only start currently active trips
		}
		m.startTrip(ctx, t)
	}
}

func (m *Manager) startTrip(parent context.Context, t gtfs.ActiveTrip) {
	m.mu.Lock()
	if _, exists := m.running[t.TripID]; exists {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.running[t.TripID] = cancel
	m.wg.Add(1)
	if m.metrics != nil {
		m.metrics.TripsStarted.Inc()
		m.metrics.ActiveTrips.Set(float64(len(m.running)))
	}
	m.mu.Unlock()

	log.Printf("starting trip %s (route %s) at %s", t.TripID, t.RouteID, t.StartTime.Format(time.RFC3339))
	go func() {
		defer m.wg.Done()
		if err := m.runTrip(ctx, t); err != nil {
			log.Printf("trip %s error: %v", t.TripID, err)
		}
		m.mu.Lock()
		delete(m.running, t.TripID)
		if m.metrics != nil {
			m.metrics.TripsFinished.Inc()
			m.metrics.ActiveTrips.Set(float64(len(m.running)))
		}
		m.mu.Unlock()
	}()
}

func (m *Manager) runTrip(ctx context.Context, t gtfs.ActiveTrip) error {
	// Load shape and stop_times
	shapePts, err := db.FetchShapePoints(ctx, m.db, t.ShapeID)
	if err != nil {
		return err
	}
	if len(shapePts) == 0 {
		// Without shape, we cannot simulate geographic position. Skip.
		return nil
	}
	cum := db.CumDistances(shapePts)
	totalDist := 0.0
	if len(cum) > 0 {
		totalDist = cum[len(cum)-1]
	}
	if totalDist == 0 {
		return nil
	}

	// Build per-stop schedule (time -> distance along shape)
	baseMidnight := time.Date(t.StartTime.Year(), t.StartTime.Month(), t.StartTime.Day(), 0, 0, 0, 0, t.StartTime.Location())
	sts, err := db.FetchStopTimes(ctx, m.db, t.TripID)
	if err != nil {
		return err
	}
	times, dists := buildSchedule(sts, baseMidnight, totalDist, shapePts, cum)
	if len(times) == 0 {
		// Fallback: use simple global interpolation if stop_times missing
		times = []time.Time{t.StartTime, t.EndTime}
		dists = []float64{0, totalDist}
	}

	tick := time.NewTicker(m.publishInterval)
	defer tick.Stop()

	routeID := t.RouteID
	tripID := t.TripID

	lastPosTime := time.Time{}
	var lastLat, lastLon float64

	nextLogIdx := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-tick.C:
			tickStart := time.Now()
			// Scale time progression by speedMultiplier relative to real wall-clock
			// Compute target absolute time within schedule domain
			if now.Before(t.StartTime) {
				continue
			}
			if now.After(t.EndTime) {
				log.Printf("finished trip %s at %s", tripID, now.Format(time.RFC3339))
				return nil
			}
			elapsed := now.Sub(t.StartTime).Seconds() * m.speedMultiplier
			targetAbs := t.StartTime.Add(time.Duration(elapsed) * time.Second)
			if targetAbs.After(t.EndTime) {
				targetAbs = t.EndTime
			}
			targetDist := interpolateDistAtTime(times, dists, targetAbs)
			for nextLogIdx < len(times) && targetAbs.After(times[nextLogIdx]) {
				log.Printf("trip %s passed keyframe %d/%d (t=%s, dist=%.1fm)", tripID, nextLogIdx+1, len(times), times[nextLogIdx].Format(time.RFC3339), dists[nextLogIdx])
				nextLogIdx++
			}
			lat, lon, bearing := db.InterpolateShape(shapePts, cum, targetDist)

			// estimate speed based on last position
			speed := 0.0
			if !lastPosTime.IsZero() {
				dt := now.Sub(lastPosTime).Seconds()
				if dt > 0 {
					// re-use haversine via small wrapper
					d := distanceMeters(lastLat, lastLon, lat, lon)
					speed = d / dt
				}
			}
			lastPosTime = now
			lastLat, lastLon = lat, lon

			pm := publisher.PositionMessage{
				TripID:    tripID,
				RouteID:   routeID,
				Timestamp: now,
				Lat:       lat,
				Lon:       lon,
				Bearing:   bearing,
				Progress:  targetDist / totalDist,
				SpeedMps:  speed,
			}
			if err := m.pub.PublishPosition(routeID, tripID, pm); err != nil {
				log.Printf("publish error for %s: %v", tripID, err)
			}
			if m.metrics != nil {
				m.metrics.TickDuration.Observe(time.Since(tickStart).Seconds())
			}
		}
	}
}

// buildSchedule constructs a time->distance schedule from stop_times.
// It prefers stop_times.shape_dist_traveled if available; otherwise
// distributes distances evenly along the shape.
func buildSchedule(sts []gtfs.StopTime, baseMidnight time.Time, totalDist float64, shapePts []gtfs.ShapePoint, cum []float64) ([]time.Time, []float64) {
	if len(sts) == 0 {
		return nil, nil
	}
	// Build keyframes: for first stop -> departure (or arrival if dep missing),
	// intermediate stops -> arrival and departure if they differ, last stop -> arrival.
	var times []time.Time
	var dists []float64
	var haveDist []bool
	nStops := len(sts)
	// helper to append a keyframe
	appendKF := func(sec int, dist float64, has bool) {
		times = append(times, baseMidnight.Add(time.Duration(sec)*time.Second))
		dists = append(dists, dist)
		haveDist = append(haveDist, has)
	}
	// compute per-stop provided distances
	stopDists := make([]float64, nStops)
	stopHas := make([]bool, nStops)
	for i, st := range sts {
		if st.ShapeDistTraveled > 0 {
			stopDists[i] = st.ShapeDistTraveled
			stopHas[i] = true
			continue
		}
		// try nearest distance along shape using stop lat/lon
		if st.StopLat != 0 || st.StopLon != 0 {
			stopDists[i] = db.NearestDistanceAlongShape(shapePts, cum, st.StopLat, st.StopLon)
			stopHas[i] = true
		}
	}
	// If none provided or computable, evenly distribute
	anyProvided := false
	for _, h := range stopHas {
		if h {
			anyProvided = true
			break
		}
	}
	if !anyProvided {
		for i := 0; i < nStops; i++ {
			if nStops > 1 {
				stopDists[i] = totalDist * float64(i) / float64(nStops-1)
			} else {
				stopDists[i] = 0
			}
			stopHas[i] = true
		}
	} else {
		// Fill missing with nearest neighbor interpolation, clamp to [0,totalDist]
		// Ensure non-decreasing
		// Forward fill
		last := 0.0
		has := false
		for i := 0; i < nStops; i++ {
			if stopHas[i] {
				last = stopDists[i]
				has = true
			} else if has {
				stopDists[i] = last
				stopHas[i] = true
			}
		}
		// Backward fill
		next := totalDist
		hasNext := false
		for i := nStops - 1; i >= 0; i-- {
			if stopHas[i] {
				next = stopDists[i]
				hasNext = true
			} else if hasNext {
				stopDists[i] = next
				stopHas[i] = true
			}
		}
		// Enforce monotonic
		prev := 0.0
		for i := 0; i < nStops; i++ {
			if !stopHas[i] { // fallback evenly if still missing
				if nStops > 1 {
					stopDists[i] = totalDist * float64(i) / float64(nStops-1)
				}
				stopHas[i] = true
			}
			if stopDists[i] < prev {
				stopDists[i] = prev
			}
			if stopDists[i] > totalDist {
				stopDists[i] = totalDist
			}
			prev = stopDists[i]
		}
	}
	for i, st := range sts {
		// first stop keyframe
		if i == 0 {
			sec := st.DepartureSec
			if sec == 0 {
				sec = st.ArrivalSec
			}
			appendKF(sec, stopDists[i], true)
			continue
		}
		// intermediate and last stops
		if st.ArrivalSec > 0 {
			appendKF(st.ArrivalSec, stopDists[i], true)
		}
		if st.DepartureSec > 0 && st.DepartureSec != st.ArrivalSec {
			appendKF(st.DepartureSec, stopDists[i], true)
		}
		if i == nStops-1 && st.ArrivalSec == 0 && st.DepartureSec > 0 {
			// Ensure last stop arrival exists
			appendKF(st.DepartureSec, stopDists[i], true)
		}
	}
	// Remove any strictly decreasing time pairs or duplicates with same time and distance
	// Sort is not necessary; GTFS times are non-decreasing by stop_sequence.
	filteredTimes := make([]time.Time, 0, len(times))
	filteredDists := make([]float64, 0, len(dists))
	var lastT time.Time
	var lastD float64
	first := true
	for i := range times {
		t := times[i]
		d := dists[i]
		if first {
			filteredTimes = append(filteredTimes, t)
			filteredDists = append(filteredDists, d)
			lastT, lastD, first = t, d, false
			continue
		}
		if t.Before(lastT) {
			continue
		}
		if t.Equal(lastT) && d == lastD {
			continue
		}
		filteredTimes = append(filteredTimes, t)
		filteredDists = append(filteredDists, d)
		lastT, lastD = t, d
	}
	return filteredTimes, filteredDists
}

func interpolateDistAtTime(times []time.Time, dists []float64, at time.Time) float64 {
	n := len(times)
	if n == 0 {
		return 0
	}
	if !at.After(times[0]) {
		return dists[0]
	}
	if !at.Before(times[n-1]) {
		return dists[n-1]
	}
	// find segment i s.t. times[i] <= at < times[i+1]
	i := 0
	for i+1 < n && at.After(times[i+1]) {
		i++
	}
	if i+1 >= n {
		return dists[n-1]
	}
	t0 := times[i]
	t1 := times[i+1]
	d0 := dists[i]
	d1 := dists[i+1]
	dt := t1.Sub(t0)
	if dt <= 0 {
		return d0
	}
	frac := float64(at.Sub(t0)) / float64(dt)
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	return d0 + (d1-d0)*frac
}

func (m *Manager) Stop() {
	if m.refreshCancel != nil {
		m.refreshCancel()
	}
	m.refreshWG.Wait()
	// cancel scheduled starts
	m.mu.Lock()
	for _, cancel := range m.scheduled {
		cancel()
	}
	m.scheduled = make(map[string]context.CancelFunc)
	m.mu.Unlock()
	m.scheduledWG.Wait()
	m.mu.Lock()
	for _, cancel := range m.running {
		cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// StartRefresher launches a background loop that periodically fetches active trips
// and starts new goroutines for newly active trips.
func (m *Manager) StartRefresher(parent context.Context) {
	if m.refreshInterval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.refreshCancel = cancel
	m.refreshWG.Add(1)
	go func() {
		defer m.refreshWG.Done()
		// immediate refresh on start
		_ = m.RefreshActive(ctx)
		ticker := time.NewTicker(m.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.RefreshActive(ctx); err != nil {
					log.Printf("refresh active trips error: %v", err)
				}
			}
		}
	}()
}

// RefreshActive queries DB for currently active trips and starts those that
// are not yet running and within their active window.
func (m *Manager) RefreshActive(ctx context.Context) error {
	now := time.Now().In(m.tz)
	trips, err := db.FetchActiveTrips(ctx, m.db, now)
	if err != nil {
		return err
	}
	for _, t := range trips {
		if now.After(t.EndTime) {
			continue
		}
		if now.Before(t.StartTime) {
			// schedule if within horizon
			if m.preloadHorizon > 0 && t.StartTime.Sub(now) <= m.preloadHorizon {
				m.scheduleTrip(ctx, t)
			}
			continue
		}
		// currently active
		m.startTrip(ctx, t)
	}
	return nil
}

func (m *Manager) scheduleTrip(parent context.Context, t gtfs.ActiveTrip) {
	m.mu.Lock()
	if _, running := m.running[t.TripID]; running {
		m.mu.Unlock()
		return
	}
	if _, exists := m.scheduled[t.TripID]; exists {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.scheduled[t.TripID] = cancel
	m.scheduledWG.Add(1)
	if m.metrics != nil {
		m.metrics.TripsScheduled.Inc()
		m.metrics.ScheduledTrips.Set(float64(len(m.scheduled)))
	}
	m.mu.Unlock()

	log.Printf("scheduled trip %s for %s", t.TripID, t.StartTime.Format(time.RFC3339))
	go func() {
		defer m.scheduledWG.Done()
		defer func() {
			m.mu.Lock()
			delete(m.scheduled, t.TripID)
			if m.metrics != nil {
				m.metrics.ScheduledTrips.Set(float64(len(m.scheduled)))
			}
			m.mu.Unlock()
		}()
		now := time.Now().In(m.tz)
		d := time.Until(t.StartTime)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		// on wake, start if still not started and before end
		m.mu.Lock()
		_, isRunning := m.running[t.TripID]
		m.mu.Unlock()
		if isRunning {
			return
		}
		now = time.Now().In(m.tz)
		if now.After(t.EndTime) {
			return
		}
		log.Printf("starting scheduled trip %s at %s", t.TripID, now.Format(time.RFC3339))
		m.startTrip(parent, t)
	}()
}

// local distance helper mirroring db.haversine without export
func distanceMeters(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000.0
	toRad := func(d float64) float64 { return d * math.Pi / 180 }
	dLat := toRad(lat2 - lat1)
	dLon := toRad(lon2 - lon1)
	a := (math.Sin(dLat/2) * math.Sin(dLat/2)) + math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}
