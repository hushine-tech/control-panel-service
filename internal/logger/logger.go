package logger

import (
	"context"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

type globalAdapter struct{}

func (globalAdapter) Info(ctx context.Context, logType, msg string)           { elog.Info(ctx, logType, msg) }
func (globalAdapter) Debug(ctx context.Context, logType, msg string)          { elog.Debug(ctx, logType, msg) }
func (globalAdapter) Warn(ctx context.Context, logType, msg string)           { elog.Warn(ctx, logType, msg) }
func (globalAdapter) Error(ctx context.Context, logType, msg string)          { elog.Error(ctx, logType, msg) }
func (globalAdapter) Fatal(ctx context.Context, logType, msg string)          { elog.Fatal(ctx, logType, msg) }
func (globalAdapter) Access(ctx context.Context, e elog.AccessLogEntry)       { elog.Access(ctx, e) }
func (globalAdapter) ExtAPI(ctx context.Context, e elog.ExtAPILogEntry)       { elog.ExtAPI(ctx, e) }
func (globalAdapter) WebSocket(ctx context.Context, e elog.WebSocketLogEntry) { elog.WebSocket(ctx, e) }
func (globalAdapter) SQL(ctx context.Context, e elog.SQLLogEntry)             { elog.SQL(ctx, e) }
func (globalAdapter) GRPCAccess(ctx context.Context, e elog.GRPCAccessLogEntry) {
	elog.GRPCAccess(ctx, e)
}
func (globalAdapter) GRPCExt(ctx context.Context, e elog.GRPCExtLogEntry)     { elog.GRPCExt(ctx, e) }
func (globalAdapter) KafkaSent(ctx context.Context, e elog.KafkaSentLogEntry) { elog.KafkaSent(ctx, e) }
func (globalAdapter) KafkaRecv(ctx context.Context, e elog.KafkaRecvLogEntry) { elog.KafkaRecv(ctx, e) }
func (globalAdapter) Close() error                                            { return nil }

var adapter globalAdapter

func Init(configPath string) error          { return elog.InitLog(configPath) }
func InitWithConfig(cfg *elog.Config) error { return elog.InitLogWithConfig(cfg) }
func Close() error                          { return elog.Close() }
func Instance() elog.Logger                 { return adapter }

func Info(ctx context.Context, logType, msg string)  { elog.Info(ctx, logType, msg) }
func Warn(ctx context.Context, logType, msg string)  { elog.Warn(ctx, logType, msg) }
func Error(ctx context.Context, logType, msg string) { elog.Error(ctx, logType, msg) }
func Debug(ctx context.Context, logType, msg string) { elog.Debug(ctx, logType, msg) }
