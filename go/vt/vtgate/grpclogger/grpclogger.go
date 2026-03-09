package grpclogger

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	query "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"

	"golang.org/x/net/context"
)

type QueryLogResult struct {
	UtcTimeNow      int64
	Keyspace        string
	TabletType      string
	VtGateQuery     *query.BoundQuery
	QueryDurationMs int64
	QueryRowCount   uint64
	Error           bool
}

var (
	FileLogger = logrus.New()

	QueryReplayLoggingEnabled = flag.Bool("query_replay_logging_enabled", false, "Whether to enable grpc query file logging or not")

	QueryReplayLogPath = flag.String("query_replay_log_path", "/vt/vtdataroot/tmp/", "The default path to log grpc requests to")

	QueryReplayLogName = flag.String("query_replay_log_name", "vtgate_queries", "The default file logging name")

	log_channel = make(chan QueryLogResult, 10000)
)

func Init(unaryInterceptors *[]grpc.UnaryServerInterceptor) {
	if *QueryReplayLoggingEnabled {
		*unaryInterceptors = append(*unaryInterceptors, loggingUnaryInterceptor)

		podName := os.Getenv("POD_NAME")
		fileNameWithPodName := *QueryReplayLogName + "-" + podName
		fileNameAndPath := *QueryReplayLogPath + fileNameWithPodName

		log.Printf("Initializing logging library GRPCLogger and logrotater")
		lumberjackLogRotater := lumberjack.Logger{
			Filename:   fileNameAndPath,
			MaxSize:    5000, // megabytes
			MaxBackups: 1500,
			MaxAge:     7,     //days
			Compress:   false, // disabled by default
		}
		FileLogger.SetFormatter(&logrus.JSONFormatter{})
		FileLogger.SetOutput(&lumberjackLogRotater)

		ticker := time.NewTicker(1 * time.Minute)
		go func() {
			for {
				select {
				case <-ticker.C:
					lumberjackLogRotater.Rotate()
				}
			}
		}()

		//kick off background go routine to watch loggging channel
		go processLogChannelData()

	} else {
		log.Println("Grpc Logging for query replay is disabled")
	}
}

func loggingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	startTime := time.Now()
	invoker, err := handler(ctx, req)
	endTime := time.Now()

	if info.FullMethod == "/vtgateservice.Vitess/Execute" {
		if p, ok := req.(*vtgate.ExecuteRequest); ok {
			if response, okResponse := invoker.(*vtgate.ExecuteResponse); okResponse {
				// extract "Burrata" from Burrata:0@master or Burrata@master
				keyspace, _, _, parseErr := topoproto.ParseDestination(p.Session.TargetString, topodatapb.TabletType_MASTER)
				if parseErr != nil {
					log.Printf("Failed to parse out keyspace from %v, err: %v", p.Session.TargetString, parseErr)
					return invoker, err
				}

				// we are only interested in the select statements for query replay
				if !isSelectQuery(p.Query.Sql) {
					return invoker, err
				}

				queryRowCount := uint64(0)
				if response != nil && response.Result != nil {
					queryRowCount = response.Result.RowsAffected
				}
				queryLogResult := QueryLogResult{
					UtcTimeNow:      startTime.UTC().UnixNano() / int64(1000000),
					Keyspace:        keyspace,
					TabletType:      p.TabletType.String(),
					VtGateQuery:     p.Query,
					QueryDurationMs: endTime.Sub(startTime).Milliseconds(),
					QueryRowCount:   queryRowCount,
					Error:           response != nil && response.Error != nil,
				}

				select {
				case log_channel <- queryLogResult:
				default:
					log.Println("Query log channel is full")
				}
			}
		}
	}

	return invoker, err
}

func isSelectQuery(query string) bool {
	sqlQuery := strings.TrimSpace(strings.ReplaceAll(strings.ToLower(query), "(", ""))
	return strings.HasPrefix(sqlQuery, "select") && !strings.Contains(sqlQuery, "for update")
}

func processLogChannelData() {
	log.Println("Listening for log replay channel data")
	for {
		select {
		case queryData := <-log_channel:
			FileLogger.WithFields(logrus.Fields{
				"utc_time_now":    queryData.UtcTimeNow,
				"keyspace":        queryData.Keyspace,
				"tablet_type":     queryData.TabletType,
				"query":           queryData.VtGateQuery.Sql,
				"query_params":    queryData.VtGateQuery.BindVariables,
				"queryDurationMs": queryData.QueryDurationMs,
				"queryRowCount":   queryData.QueryRowCount,
				"error":           queryData.Error,
			}).Info("")
			break
		}
	}
}
