FROM golang:1.23.2

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build nats_sync/cmd/main.go -o nats_sync

ENTRYPOINT [ "/app/nats_sync" ]
CMD [ ]