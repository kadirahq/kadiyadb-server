package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb-transport"
)

const (
	address = "localhost:1234"
	tmpdir  = "/tmp/test-server/"
)

var (
	nextID uint32
)

func init() {
	if err := os.RemoveAll(tmpdir); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(tmpdir+"test", 0777); err != nil {
		panic(err)
	}

	data := []byte(`
	{
		"duration": "3h",
		"retention": "24h",
		"resolution": "1m",
		"maxROEpochs": 2,
		"maxRWEpochs": 2
	}`)

	filepath := path.Join(tmpdir, "test/params.json")
	if err := ioutil.WriteFile(filepath, data, 0777); err != nil {
		panic(err)
	}

	go Listen(address, tmpdir)

	c, err := transport.Dial(address)
	for err != nil {
		c, err = transport.Dial(address)
	}

	c.Close()
}

func TestDial(t *testing.T) {
	c, err := transport.Dial(address)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTrack(t *testing.T) {
	c, err := transport.Dial(address)
	if err != nil {
		t.Fatal(err)
	}

	req := &protocol.Request{
		Id: atomic.AddUint32(&nextID, 1),
		Req: &protocol.Request_Track{
			Track: &protocol.ReqTrack{
				Database: "test",
				Time:     10,
				Total:    20,
				Count:    10,
				Fields:   []string{"a", "b", "c"},
			},
		},
	}

	if err := c.Send(req); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	res := &protocol.Response{}
	if err := c.Recv(res); err != nil {
		t.Fatal(err)
	}

	fmt.Println("res", res)
	// TODO verify the track was successful
	// TODO verify response message content

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFetch(t *testing.T) {
	c, err := transport.Dial(address)
	if err != nil {
		t.Fatal(err)
	}

	req := &protocol.Request{
		Id: atomic.AddUint32(&nextID, 1),
		Req: &protocol.Request_Fetch{
			Fetch: &protocol.ReqFetch{
				Database: "test",
				From:     0,
				To:       6e10,
				Fields:   []string{"a", "b", "c"},
			},
		},
	}

	if err := c.Send(req); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	res := &protocol.Response{}
	if err := c.Recv(res); err != nil {
		t.Fatal(err)
	}

	fmt.Println("res", res)
	// TODO verify response message content

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
