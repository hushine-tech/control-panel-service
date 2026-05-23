package runtimechannel

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ValidationGap struct {
	StartMS       int64
	EndMS         int64
	ExpectedCount int64
}

type KlineValidationResult struct {
	OK            bool
	ExpectedCount int64
	ActualCount   int64
	MissingGaps   []ValidationGap
	Reason        string
}

func ValidateKlineRows(interval string, startMS, endMS int64, rows []KlineRow) KlineValidationResult {
	// 这里按 startMS 相对网格验证；RPC 调用方负责先校验 UTC epoch interval 对齐。
	stepMS, err := intervalStepMS(interval)
	if err != nil {
		return KlineValidationResult{Reason: err.Error()}
	}
	if startMS <= 0 || endMS <= 0 || endMS <= startMS {
		return KlineValidationResult{Reason: "endMS must be after startMS"}
	}
	if (endMS-startMS)%stepMS != 0 {
		return KlineValidationResult{
			Reason: fmt.Sprintf("startMS and endMS must align to interval %q", interval),
		}
	}

	expected := (endMS - startMS) / stepMS
	seen := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		if row.OpenTime < startMS || row.OpenTime >= endMS {
			continue
		}
		seen[row.OpenTime] = struct{}{}
	}

	gaps := make([]ValidationGap, 0)
	var missingStart int64 = -1
	for cursor := startMS; cursor < endMS; cursor += stepMS {
		if _, ok := seen[cursor]; ok {
			if missingStart >= 0 {
				gaps = append(gaps, ValidationGap{
					StartMS:       missingStart,
					EndMS:         cursor,
					ExpectedCount: (cursor - missingStart) / stepMS,
				})
				missingStart = -1
			}
			continue
		}
		if missingStart < 0 {
			missingStart = cursor
		}
	}
	if missingStart >= 0 {
		gaps = append(gaps, ValidationGap{
			StartMS:       missingStart,
			EndMS:         endMS,
			ExpectedCount: (endMS - missingStart) / stepMS,
		})
	}

	result := KlineValidationResult{
		OK:            len(gaps) == 0,
		ExpectedCount: expected,
		ActualCount:   int64(len(seen)),
		MissingGaps:   gaps,
	}
	if !result.OK {
		result.Reason = "missing kline rows"
	}
	return result
}

func intervalStepMS(interval string) (int64, error) {
	interval = strings.TrimSpace(interval)
	if len(interval) < 2 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	n, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("interval %q is invalid", interval)
	}
	var d time.Duration
	switch interval[len(interval)-1] {
	case 's':
		d = time.Duration(n) * time.Second
	case 'm':
		d = time.Duration(n) * time.Minute
	case 'h':
		d = time.Duration(n) * time.Hour
	case 'd':
		d = time.Duration(n) * 24 * time.Hour
	default:
		return 0, fmt.Errorf("interval %q is not supported for coverage validation; supported units: s/m/h/d", interval)
	}
	return int64(d / time.Millisecond), nil
}
