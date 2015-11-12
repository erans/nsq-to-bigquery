package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	ver           = "0.1"
	bigQueryScope = "https://www.googleapis.com/auth/bigquery"
)

var (
	showVersion     = flag.Bool("version", false, "print version string")
	topic           = flag.String("topic", "", "NSQ topic")
	channel         = flag.String("channel", "", "NSQ channel")
	appID           = flag.String("app", "", "The App/Project ID")
	dataSet         = flag.String("dataset", "", "The dataset to send the data to")
	table           = flag.String("table", "", "The table name to send the data to")
	maxInFlight     = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	credentialsFile = flag.String("credentials-file", "", "credentials file to access BigQuery")

	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	bq *bigquery.Service
)

type bigQueryHandler struct {
	MaxRows         int
	TimeBeforeFlush time.Duration
}

func newBigQueryHandler(maxRowsBeforeFlush int, numberOfMinutesBeforeFlush int64) bigQueryHandler {
	return bigQueryHandler{MaxRows: maxRowsBeforeFlush, TimeBeforeFlush: time.Second * time.Duration(numberOfMinutesBeforeFlush*60)}
}

func (th *bigQueryHandler) HandleMessage(m *nsq.Message) error {
	var data map[string]bigquery.JsonValue
	if err := json.Unmarshal(m.Body, &data); err != nil {
		return err
	}

	var rows []*bigquery.TableDataInsertAllRequestRows

	row := &bigquery.TableDataInsertAllRequestRows{
		Json: make(map[string]bigquery.JsonValue, 0),
	}

	for key, value := range data {
		row.Json[key] = value
	}

	rows = append(rows, row)

	req := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}

	call := bq.Tabledata.InsertAll(*appID, *dataSet, *table, req)

	go func() {
		resp, err := call.Do()
		if err != nil {
			log.Fatal(err)
		}

		if resp.HTTPStatusCode != 200 {
			if respJSON, err := resp.MarshalJSON(); err == nil {
				fmt.Printf("%s\n", string(respJSON))
			} else {
				fmt.Printf("Failed to send data to big query and failed to serialize the response and log it to error")
			}

		}
	}()

	return nil
}

func (th *bigQueryHandler) Flush() error {
	return nil
}

func initBigQueryService(credentialsFileName string) error {
	var client *http.Client

	credentialsData, err := ioutil.ReadFile(credentialsFileName)
	if err != nil {
		return err
	}

	var conf *jwt.Config
	conf, err = google.JWTConfigFromJSON(credentialsData, bigQueryScope)
	if err != nil {
		return err
	}

	client = conf.Client(oauth2.NoContext)

	if bq, err = bigquery.New(client); err != nil {
		return err
	}

	return nil
}

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

func main() {
	cfg := nsq.NewConfig()

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_bigquery/%s nsq/%s go-nsq/%s", ver, version.Binary, nsq.VERSION)
		return
	}

	if *channel == "" {
		log.Fatal("-channel is required")
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *credentialsFile == "" {
		log.Fatal("-credentials-file is required")
	}

	if *appID == "" {
		log.Fatal("-app is required")
	}

	if *dataSet == "" {
		log.Fatal("-dataSet is required")
	}

	if *table == "" {
		log.Fatal("-table is required")
	}

	if err := initBigQueryService(*credentialsFile); err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("nsq_to_bigquery/%s nsq/%s go-nsq/%s", ver, version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(&bigQueryHandler{})
	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
