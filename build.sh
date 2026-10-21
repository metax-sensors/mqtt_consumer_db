docker run --rm \
	-v "${PWD}":/telegraf_plugin \
	-v "${PWD}/../telegraf/plugin":/build \
	golang:1.27-alpine \
	/bin/sh -c "cd /telegraf_plugin && GOOS='linux' && GOARCH='amd64' && export GOOS GOARCH && go build -o /build/mqtt_consumer_db ./main.go"