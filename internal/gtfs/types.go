package gtfs

import "time"

type Trip struct {
	TripID    string
	RouteID   string
	ShapeID   string
	ServiceID string
}

type ActiveTrip struct {
	Trip
	StartTime time.Time // absolute time (service day, local TZ)
	EndTime   time.Time // absolute time
}

type StopTime struct {
	StopSequence      int
	ArrivalSec        int     // seconds since midnight (can exceed 24h)
	DepartureSec      int     // seconds since midnight (can exceed 24h)
	ShapeDistTraveled float64 // meters, if available; 0 if missing
	StopID            string
	StopLat           float64
	StopLon           float64
}

type ShapePoint struct {
	Lat          float64
	Lon          float64
	Sequence     int
	DistTraveled float64 // meters, if available; 0 if missing
}

type Position struct {
	Lat        float64
	Lon        float64
	BearingDeg float64
	Progress   float64 // 0..1
	SpeedMps   float64
}
