# cd $GOPATH/src/github.com/olric-data/olric/
# docker build -f docker/Dockerfile -t olricio/olric-dev .
# docker push olricio/olric-dev:latest

FROM golang:latest

WORKDIR /src/
COPY go.sum /src/
COPY go.mod /src/

# Dependencies of Olric
RUN go mod download

# Install Delve debugger
RUN go get github.com/go-delve/delve/cmd/dlv