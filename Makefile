default: build

build:
	CGO_ENABLED=0 go build -ldflags "-s" -a -installsuffix cgo -o nsq_to_bigquery .
