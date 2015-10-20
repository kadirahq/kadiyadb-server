# KadiyaDB

> Status: Alpha

KadiyaDB is a time-series database designed for storing real-time metrics.

## Getting Started

KadiyaDB is written in Go and like many Go programs, the server is go-gettable.
To build the server from the source code, `go get` the server package/program.

```shell
go get -u github.com/kadirahq/kadiyadb-server
```

You can also start a database server using our pre-built Docker container.
The docker image is based on [alpine linux](http://www.alpinelinux.org/)
therefore the image is quite small.

``` shell
docker run -d \
  -p 8000:8000 \
  -v /tmp/data:/data \
  --cap-add=IPC_LOCK \
  kadirahq/kadiyadb-server:latest
```
