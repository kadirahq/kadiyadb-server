package main

import (
	"errors"
	"time"

	"github.com/kadirahq/kadiyadb"
	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb-transport"
)

const (
	// SyncInterval is the time between database syncs
	// client responses are flushed every SyncInterval
	SyncInterval = 100 * time.Millisecond
)

var (
	// ErrUnknownDB : requested db doesn't exist
	ErrUnknownDB = errors.New("unknown database")

	// ErrCorruptMsg : the message data is corrupt
	ErrCorruptMsg = errors.New("corrupt message")
)

// Listener ...
type Listener struct {
	listener  *transport.Listener
	databases map[string]*kadiyadb.DB
}

// Listen ...
func Listen(addr, dir string) (err error) {
	l := &Listener{
		databases: kadiyadb.LoadAll(dir),
	}

	go l.syncDatabases()

	l.listener = transport.NewListener(l.handle)
	return l.listener.Listen(addr)
}

func (l *Listener) syncDatabases() {
	for {
		for _, db := range l.databases {
			db.Sync()
		}

		// FIXME there's a race between db syncs and client write flushes
		// one possible solution is to perform client flushes in 2 steps
		// step one swaps buffers so new responses go to next write flush
		// step two actually flushes the writes to the tcp connection
		l.listener.Flush()

		time.Sleep(SyncInterval)
	}
}

func (l *Listener) handle(c *transport.Conn) (err error) {
	msg := &protocol.Request{}
	if err := c.Recv(msg); err != nil {
		return err
	}

	switch msg.Req.(type) {
	case *protocol.Request_Track:
		go l.track(c, msg)
	case *protocol.Request_Fetch:
		go l.fetch(c, msg)
	}

	return nil
}

func (l *Listener) track(c *transport.Conn, msg *protocol.Request) {
	req := msg.GetTrack()
	res := &protocol.ResTrack{}

	defer func() {
		c.Send(&protocol.Response{
			Id:  msg.Id,
			Res: &protocol.Response_Track{Track: res},
		})
	}()

	db, ok := l.databases[req.Database]
	if !ok {
		res.Error = ErrUnknownDB.Error()
		return
	}

	if err := db.Track(req.Time, req.Fields, req.Total, req.Count); err != nil {
		res.Error = err.Error()
		return
	}
}

func (l *Listener) fetch(c *transport.Conn, msg *protocol.Request) {
	req := msg.GetFetch()
	res := &protocol.ResFetch{}

	defer func() {
		c.Send(&protocol.Response{
			Id:  msg.Id,
			Res: &protocol.Response_Fetch{Fetch: res},
		})
	}()

	db, ok := l.databases[req.Database]
	if !ok {
		res.Error = ErrUnknownDB.Error()
		return
	}

	db.Fetch(req.From, req.To, req.Fields, func(c []*protocol.Chunk, err error) {
		if err != nil {
			res.Error = err.Error()
			return
		}

		res.Chunks = c
	})
}
