package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type FieldValue struct {
	Time  time.Time
	Value interface{}
}

type FastLastValueReader struct {
	bucket string
	org    string
	client influxdb2.Client
	fields []string
}

func NewFastLastValueReader() *FastLastValueReader {
	const (
		url   = "http://influxdb.telemetry.ubcsolar.com"
		token = "s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA=="
		org   = "8a0b66d77a331e96"
	)

	return &FastLastValueReader{
		bucket: "CAN_log",
		org:    org,
		client: influxdb2.NewClient(url, token),
		fields: []string{
			"TotalPackVoltage",
			"PackCurrent",
			"VehicleVelocity",
			"AcceleratorPosition",
			"MechBrakePressed",
			"BatteryCurrent",
			"BatteryVoltage",
		},
	}
}

func (r *FastLastValueReader) Close() {
	r.client.Close()
}

func fluxTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func (r *FastLastValueReader) buildQuery(stopTime time.Time, lookback time.Duration) string {
	startTime := stopTime.Add(-lookback)

	clauses := make([]string, 0, len(r.fields))
	for _, f := range r.fields {
		clauses = append(clauses, fmt.Sprintf(`r["_field"] == "%s"`, f))
	}
	fieldFilter := strings.Join(clauses, " or ")

	return fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => %s)
  |> last()
  |> keep(columns: ["_field", "_value", "_time"])
`, r.bucket, fluxTime(startTime), fluxTime(stopTime), fieldFilter)
}

func (r *FastLastValueReader) GetLastValuesBefore(
	ctx context.Context,
	timestamp time.Time,
	lookback time.Duration,
) map[string]*FieldValue {
	out := make(map[string]*FieldValue, len(r.fields))
	for _, f := range r.fields {
		out[f] = nil
	}

	queryAPI := r.client.QueryAPI(r.org)
	query := r.buildQuery(timestamp, lookback)

	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return out
	}
	defer result.Close()

	for result.Next() {
		rec := result.Record()
		fieldAny := rec.ValueByKey("_field")
		field, ok := fieldAny.(string)
		if !ok {
			continue
		}

		if _, exists := out[field]; !exists {
			continue
		}

		out[field] = &FieldValue{
			Time:  rec.Time(),
			Value: rec.Value(),
		}
	}

	if result.Err() != nil {
		return out
	}

	return out
}

func clearScreen() {
	fmt.Print("\033[H\033[J")
}

func main() {
	reader := NewFastLastValueReader()
	defer reader.Close()

	startTime := time.Now().UTC()
	queryStartTime := time.Date(2024, 7, 16, 10, 0, 0, 0, time.UTC)
	lookback := 1 * time.Second
	period := 50 * time.Millisecond

	ctx := context.Background()

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		loopStart := time.Now()

		now := time.Now().UTC()
		timeElapsed := now.Sub(startTime)
		queryTime := queryStartTime.Add(timeElapsed)

		values := reader.GetLastValuesBefore(ctx, queryTime, lookback)
		totalTimeMs := float64(time.Since(loopStart).Microseconds()) / 1000.0

		clearScreen()
		fmt.Println("=== Live Telemetry (Last Values) ===")
		fmt.Printf("Query time: %.2f ms\n\n", totalTimeMs)

		for _, field := range reader.fields {
			data := values[field]
			if data == nil {
				fmt.Printf("%s: None\n", field)
			} else {
				fmt.Printf("%s: %v @ %s\n",
					field,
					data.Value,
					data.Time.UTC().Format(time.RFC3339Nano),
				)
			}
		}

		// maintain roughly the same pacing as your Python loop
		elapsed := time.Since(loopStart)
		if elapsed < period {
			select {
			case <-time.After(period - elapsed):
			case <-ctx.Done():
				log.Println("stopped")
				return
			}
		} else {
			select {
			case <-ticker.C:
			default:
			}
		}
	}
}