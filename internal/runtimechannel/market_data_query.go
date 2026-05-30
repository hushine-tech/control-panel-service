package runtimechannel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

const defaultKlineFetchLimit = 1000
const maxKlineFetchLimit = 5000
const defaultKlineConnectTimeoutSeconds = 5
const defaultKlineQueryAttempts = 3

var safeIdent = regexp.MustCompile(`^[a-z0-9_]+$`)

type MarketDataQueryConfig struct {
	Host                  string
	Port                  int
	User                  string
	Password              string
	Database              string
	SSLMode               string
	ConnectTimeoutSeconds int
}

type MarketDataQuery struct {
	cfg MarketDataQueryConfig
}

type KlineQuery struct {
	Exchange    string
	Market      string
	Symbol      string
	Interval    string
	StartTimeMS int64
	EndTimeMS   int64
	Limit       int
}

type KlineRow struct {
	Exchange  string
	Market    string
	Symbol    string
	Interval  string
	OpenTime  int64
	CloseTime int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Timestamp int64
}

func NewMarketDataQuery(cfg MarketDataQueryConfig) *MarketDataQuery {
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.Database == "" {
		cfg.Database = "binance_{year}"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.ConnectTimeoutSeconds <= 0 {
		cfg.ConnectTimeoutSeconds = defaultKlineConnectTimeoutSeconds
	}
	return &MarketDataQuery{cfg: cfg}
}

func (q *MarketDataQuery) FetchKlines(ctx context.Context, req KlineQuery) ([]KlineRow, error) {
	if q == nil {
		return nil, fmt.Errorf("market-data query is not configured")
	}
	req = normalizeKlineQuery(req)
	if err := validateKlineQuery(req); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultKlineFetchLimit
	}
	if limit > maxKlineFetchLimit {
		limit = maxKlineFetchLimit
	}

	out := make([]KlineRow, 0, limit)
	table := klineTableName(req.Market, req.Symbol, req.Interval)
	for _, year := range yearsInRange(req.StartTimeMS, req.EndTimeMS) {
		if len(out) >= limit {
			break
		}
		dsn, err := q.dsnForYear(req.Exchange, year)
		if err != nil {
			return nil, err
		}
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, fmt.Errorf("open market-data db: %w", err)
		}
		rows, err := queryRowsWithRetry(ctx, db, fmt.Sprintf(`
			SELECT symbol, open_time, close_time, open, high, low, close, volume
			FROM %s
			WHERE symbol = $1
			  AND open_time >= to_timestamp($2/1000.0)
			  AND open_time < to_timestamp($3/1000.0)
			ORDER BY open_time ASC
			LIMIT $4
		`, quoteIdent(table)), req.Symbol, req.StartTimeMS, req.EndTimeMS, limit-len(out))
		if err != nil {
			_ = db.Close()
			if isMissingMarketDataStorageError(err) {
				continue
			}
			return nil, fmt.Errorf("query klines %s/%s/%s: %w", req.Market, req.Symbol, req.Interval, err)
		}
		for rows.Next() {
			var (
				row       KlineRow
				openTime  time.Time
				closeTime time.Time
			)
			if err := rows.Scan(
				&row.Symbol,
				&openTime,
				&closeTime,
				&row.Open,
				&row.High,
				&row.Low,
				&row.Close,
				&row.Volume,
			); err != nil {
				_ = rows.Close()
				_ = db.Close()
				return nil, fmt.Errorf("scan kline: %w", err)
			}
			row.Exchange = req.Exchange
			row.Market = req.Market
			row.Interval = req.Interval
			row.OpenTime = openTime.UTC().UnixMilli()
			row.CloseTime = closeTime.UTC().UnixMilli()
			row.Timestamp = row.CloseTime
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			_ = db.Close()
			return nil, fmt.Errorf("iterate klines: %w", err)
		}
		_ = rows.Close()
		_ = db.Close()
	}
	return out, nil
}

func isMissingMarketDataStorageError(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}
	switch string(pqErr.Code) {
	case "3D000", // invalid_catalog_name: yearly database does not exist yet.
		"42P01": // undefined_table: symbol/interval table does not exist yet.
		return true
	default:
		return false
	}
}

func queryRowsWithRetry(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	var err error
	for attempt := 1; attempt <= defaultKlineQueryAttempts; attempt++ {
		var rows *sql.Rows
		rows, err = db.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}
		if ctx.Err() != nil || isMissingMarketDataStorageError(err) || !isTransientMarketDataQueryError(err) || attempt == defaultKlineQueryAttempts {
			return nil, err
		}
		timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func isTransientMarketDataQueryError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"dial tcp",
		"i/o timeout",
		"operation timed out",
		"connection refused",
		"connection reset",
		"server closed the connection unexpectedly",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func (q *MarketDataQuery) dsnForYear(exchange string, year int) (string, error) {
	dbName, err := databaseNameForYear(q.cfg.Database, exchange, year)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		q.cfg.Host,
		q.cfg.Port,
		q.cfg.User,
		q.cfg.Password,
		dbName,
		q.cfg.SSLMode,
		q.cfg.ConnectTimeoutSeconds,
	), nil
}

func databaseNameForYear(template, exchange string, year int) (string, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	dbName := strings.TrimSpace(template)
	if dbName == "" {
		dbName = "{exchange}_{year}"
	}
	if exchange == "" {
		return "", fmt.Errorf("exchange is required")
	}
	if year < 1970 {
		return "", fmt.Errorf("year %d is invalid", year)
	}
	if dbName == exchange {
		return "", fmt.Errorf("fixed exchange database %q is not a supported market-data read source", dbName)
	}
	if strings.Contains(dbName, "{exchange}") {
		dbName = strings.ReplaceAll(dbName, "{exchange}", exchange)
	}
	if strings.Contains(dbName, "{year}") {
		dbName = strings.ReplaceAll(dbName, "{year}", fmt.Sprintf("%d", year))
		return dbName, nil
	}
	return "", fmt.Errorf("market-data database template %q must include {year}", template)
}

func normalizeKlineQuery(req KlineQuery) KlineQuery {
	req.Exchange = strings.ToLower(strings.TrimSpace(req.Exchange))
	if req.Exchange == "" {
		req.Exchange = "binance"
	}
	req.Market = strings.ToLower(strings.TrimSpace(req.Market))
	req.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	req.Interval = strings.ToLower(strings.TrimSpace(req.Interval))
	return req
}

func validateKlineQuery(req KlineQuery) error {
	if req.Exchange != "binance" {
		return fmt.Errorf("only exchange=binance is supported, got %q", req.Exchange)
	}
	if req.Market != "spot" && req.Market != "futures" {
		return fmt.Errorf("market must be spot or futures, got %q", req.Market)
	}
	if req.Symbol == "" || !safeIdent.MatchString(strings.ToLower(req.Symbol)) {
		return fmt.Errorf("invalid symbol %q", req.Symbol)
	}
	if req.Interval == "" || !safeIdent.MatchString(req.Interval) {
		return fmt.Errorf("invalid interval %q", req.Interval)
	}
	if req.StartTimeMS <= 0 || req.EndTimeMS <= 0 || req.EndTimeMS <= req.StartTimeMS {
		return fmt.Errorf("invalid time range")
	}
	return nil
}

func klineTableName(market, symbol, interval string) string {
	return fmt.Sprintf("%s_klines_%s_%s", market, strings.ToLower(symbol), strings.ToLower(interval))
}

func quoteIdent(name string) string {
	if !safeIdent.MatchString(name) {
		panic("unsafe identifier")
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func yearsInRange(startMS, endMS int64) []int {
	start := time.UnixMilli(startMS).UTC().Year()
	end := time.UnixMilli(endMS - 1).UTC().Year()
	out := make([]int, 0, end-start+1)
	for year := start; year <= end; year++ {
		out = append(out, year)
	}
	return out
}
