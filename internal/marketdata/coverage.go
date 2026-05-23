package marketdata

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
)

func intervalDuration(interval string) (time.Duration, error) {
	if len(interval) < 2 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	n, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	switch interval[len(interval)-1] {
	case 's':
		return time.Duration(n) * time.Second, nil
	case 'm':
		return time.Duration(n) * time.Minute, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("interval %q is not supported for coverage; supported units: s/m/h/d", interval)
	}
}

func expectedCount(startAt, endAt time.Time, interval string) (int64, error) {
	startAt = startAt.UTC()
	endAt = endAt.UTC()
	if !endAt.After(startAt) {
		return 0, fmt.Errorf("end_at must be after start_at")
	}
	d, err := intervalDuration(interval)
	if err != nil {
		return 0, err
	}
	diff := endAt.Sub(startAt)
	if !isAlignedToInterval(startAt, d) || !isAlignedToInterval(endAt, d) {
		return 0, fmt.Errorf("start_at and end_at must align to interval %q", interval)
	}
	if diff%d != 0 {
		return 0, fmt.Errorf("range must be an exact multiple of interval %q", interval)
	}
	return int64(diff / d), nil
}

func isAlignedToInterval(t time.Time, d time.Duration) bool {
	n := int64(d)
	if n <= 0 {
		return false
	}
	return t.UTC().UnixNano()%n == 0
}

func computeMissingSegments(
	startAt, endAt time.Time,
	interval string,
	covered []domain.MarketDataCoverageSegment,
) ([]domain.MarketDataTimeRange, error) {
	startAt = startAt.UTC()
	endAt = endAt.UTC()
	if !endAt.After(startAt) {
		return nil, fmt.Errorf("end_at must be after start_at")
	}

	segments := make([]domain.MarketDataCoverageSegment, 0, len(covered))
	for _, seg := range covered {
		if !seg.EndAt.After(startAt) || !seg.StartAt.Before(endAt) {
			continue
		}
		seg.StartAt = maxTime(seg.StartAt.UTC(), startAt)
		seg.EndAt = minTime(seg.EndAt.UTC(), endAt)
		if seg.EndAt.After(seg.StartAt) {
			segments = append(segments, seg)
		}
	}
	sort.SliceStable(segments, func(i, j int) bool {
		if segments[i].StartAt.Equal(segments[j].StartAt) {
			return segments[i].EndAt.Before(segments[j].EndAt)
		}
		return segments[i].StartAt.Before(segments[j].StartAt)
	})

	var missing []domain.MarketDataTimeRange
	cursor := startAt
	for _, seg := range segments {
		if seg.StartAt.After(cursor) {
			n, err := expectedCount(cursor, seg.StartAt, interval)
			if err != nil {
				return nil, err
			}
			missing = append(missing, domain.MarketDataTimeRange{
				StartAt:       cursor,
				EndAt:         seg.StartAt,
				ExpectedCount: n,
			})
		}
		if seg.EndAt.After(cursor) {
			cursor = seg.EndAt
		}
	}
	if cursor.Before(endAt) {
		n, err := expectedCount(cursor, endAt, interval)
		if err != nil {
			return nil, err
		}
		missing = append(missing, domain.MarketDataTimeRange{
			StartAt:       cursor,
			EndAt:         endAt,
			ExpectedCount: n,
		})
	}
	return missing, nil
}

func mergeCoverageForQuery(
	key domain.StreamKey,
	startAt, endAt time.Time,
	covered []domain.MarketDataCoverageSegment,
) ([]domain.MarketDataCoverageSegment, error) {
	startAt = startAt.UTC()
	endAt = endAt.UTC()
	if !endAt.After(startAt) {
		return nil, fmt.Errorf("end_at must be after start_at")
	}

	segments := make([]domain.MarketDataCoverageSegment, 0, len(covered))
	for _, seg := range covered {
		if seg.Key != key {
			continue
		}
		if !seg.EndAt.After(startAt) || !seg.StartAt.Before(endAt) {
			continue
		}
		seg.StartAt = maxTime(seg.StartAt.UTC(), startAt)
		seg.EndAt = minTime(seg.EndAt.UTC(), endAt)
		if seg.EndAt.After(seg.StartAt) {
			segments = append(segments, splitCoverageSegmentByYear(seg)...)
		}
	}
	sort.SliceStable(segments, func(i, j int) bool {
		if segments[i].StartAt.Equal(segments[j].StartAt) {
			return segments[i].EndAt.Before(segments[j].EndAt)
		}
		return segments[i].StartAt.Before(segments[j].StartAt)
	})

	out := make([]domain.MarketDataCoverageSegment, 0, len(segments))
	for _, seg := range segments {
		seg.RowCount = 0
		rowCount, err := expectedCount(seg.StartAt, seg.EndAt, key.Interval)
		if err != nil {
			return nil, err
		}
		seg.RowCount = rowCount
		if len(out) == 0 {
			out = append(out, seg)
			continue
		}
		last := &out[len(out)-1]
		if last.Year == seg.Year && !seg.StartAt.After(last.EndAt) {
			if seg.EndAt.After(last.EndAt) {
				last.EndAt = seg.EndAt
			}
			last.Source = mergeCoverageSource(last.Source, seg.Source)
			rowCount, err := expectedCount(last.StartAt, last.EndAt, key.Interval)
			if err != nil {
				return nil, err
			}
			last.RowCount = rowCount
			if seg.UpdatedAt.After(last.UpdatedAt) {
				last.UpdatedAt = seg.UpdatedAt
			}
			continue
		}
		out = append(out, seg)
	}
	return out, nil
}

func coverageSegmentsFromKlineRows(
	key domain.StreamKey,
	startAt, endAt time.Time,
	rows []runtimechannel.KlineRow,
	source string,
) ([]domain.MarketDataCoverageSegment, error) {
	step, err := intervalDuration(key.Interval)
	if err != nil {
		return nil, err
	}
	stepMS := int64(step / time.Millisecond)
	startMS := startAt.UTC().UnixMilli()
	endMS := endAt.UTC().UnixMilli()
	if stepMS <= 0 || endMS <= startMS {
		return nil, fmt.Errorf("invalid coverage range")
	}

	seen := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		openMS := row.OpenTime
		if openMS < startMS || openMS >= endMS {
			continue
		}
		if (openMS-startMS)%stepMS != 0 {
			continue
		}
		seen[openMS] = struct{}{}
	}
	if len(seen) == 0 {
		return nil, nil
	}

	openTimes := make([]int64, 0, len(seen))
	for openMS := range seen {
		openTimes = append(openTimes, openMS)
	}
	sort.Slice(openTimes, func(i, j int) bool { return openTimes[i] < openTimes[j] })

	now := time.Now().UTC()
	segments := make([]domain.MarketDataCoverageSegment, 0)
	var current *domain.MarketDataCoverageSegment
	var previousMS int64
	for _, openMS := range openTimes {
		openAt := time.UnixMilli(openMS).UTC()
		end := openAt.Add(step)
		if current == nil || openMS != previousMS+stepMS {
			segments = append(segments, domain.MarketDataCoverageSegment{
				Key:       key,
				Year:      int32(openAt.Year()),
				StartAt:   openAt,
				EndAt:     end,
				RowCount:  1,
				Source:    source,
				UpdatedAt: now,
			})
			current = &segments[len(segments)-1]
		} else {
			current.EndAt = end
			current.RowCount++
		}
		previousMS = openMS
	}

	out := make([]domain.MarketDataCoverageSegment, 0, len(segments))
	for _, seg := range segments {
		out = append(out, splitCoverageSegmentByYear(seg)...)
	}
	for i := range out {
		rowCount, err := expectedCount(out[i].StartAt, out[i].EndAt, key.Interval)
		if err != nil {
			return nil, err
		}
		out[i].RowCount = rowCount
	}
	return out, nil
}

func splitCoverageSegmentByYear(seg domain.MarketDataCoverageSegment) []domain.MarketDataCoverageSegment {
	seg.StartAt = seg.StartAt.UTC()
	seg.EndAt = seg.EndAt.UTC()
	if !seg.EndAt.After(seg.StartAt) {
		return nil
	}

	out := make([]domain.MarketDataCoverageSegment, 0, 1)
	cursor := seg.StartAt
	for cursor.Before(seg.EndAt) {
		nextYear := time.Date(cursor.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
		partEnd := minTime(seg.EndAt, nextYear)
		part := seg
		part.StartAt = cursor
		part.EndAt = partEnd
		part.Year = int32(cursor.Year())
		out = append(out, part)
		cursor = partEnd
	}
	return out
}

func mergeCoverageSource(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" || a == b {
		return a
	}
	return "mixed"
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
