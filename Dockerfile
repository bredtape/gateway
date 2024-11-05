FROM golang:1.23.2

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build sync/cmd/main.go

#ENTRYPOINT [  "go", "run", "sync/cmd/main.go" ]
ENTRYPOINT [  "/app/main" ]
CMD [ ]