package log

import (
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// @Author KHighness
// @Update 2023-04-07

const (
	logDir            = "log/"
	logSuffix         = ".log"
	logTimeFormat     = "2006-01-02 15:04:05.000"
	consoleFileLength = 20
)

// NewZapLogger creates a zap.Logger.
func NewZapLogger(log string) *zap.Logger {
	var core zapcore.Core
	// fileCore := zapcore.NewCore(zapFileEncoder(), zapWriteSyncer(log), zapLevelEnabler())
	consoleCore := zapcore.NewCore(zapConsoleEncoder(), os.Stdout, zapLevelEnabler())
	// core = zapcore.NewTee(fileCore, consoleCore)
	core = zapcore.NewTee(consoleCore)
	return zap.New(core, zap.AddCaller())
}

func zapLevelEnabler() zapcore.Level {
	return zapcore.DebugLevel
}

func zapEncodeConfig(encodeCaller zapcore.CallerEncoder) zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "ts",
		NameKey:        "logger",
		CallerKey:      "caller_line",
		StacktraceKey:  "stacktrace",
		LineEnding:     "\n",
		EncodeLevel:    zapEncodeLevel,
		EncodeTime:     zapEncodeTime,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   encodeCaller,
	}
}

func zapFileEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zapEncodeConfig(zapFileEncodeCaller))
}

func zapConsoleEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zapEncodeConfig(zapConsoleEncodeCaller))
}

func zapEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

func zapEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(logTimeFormat))
}

func zapFileEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(caller.TrimmedPath())
}

func zapConsoleEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	file := caller.File
	lo, hi := strings.LastIndex(caller.File, "/")+1, strings.LastIndex(caller.File, ".")
	if lo < hi {
		file = file[lo:hi]
	}
	fullSpaceNum := consoleFileLength - len(file)
	if fullSpaceNum < 0 {
		file = file[-fullSpaceNum:]
	}
	if fullSpaceNum > 0 {
		file = file + strings.Repeat(" ", fullSpaceNum)
	}
	enc.AppendString("[" + file + "]")
}

func zapWriteSyncer(log string) zapcore.WriteSyncer {
	_ = os.Mkdir(logDir, 0777)
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logDir + log + logSuffix,
		MaxSize:    1000,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
	}
	return zapcore.AddSync(lumberJackLogger)
}
