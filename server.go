package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/kadirahq/go-tools/function"
	"github.com/kadirahq/kadiyadb"
	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb-transport"
)

// message types
const (
	MsgTypeTrack = iota + 1
	MsgTypeFetch
)

const (
	// syncPeriod is the time between database syncs in milliseconds
	syncPeriod = 100
)

var (
	// ErrUnknownDB : requested db doesn't exist
	ErrUnknownDB = errors.New("unknown database")

	// ErrCorruptMsg : the message data is corrupt
	ErrCorruptMsg = errors.New("corrupt message")
)

// Server is a kadiradb server
type Server struct {
	trServer *transport.Server
	dbs      map[string]*kadiyadb.DB
	sync     *function.Group
}

// NewServer create a transport connection that clients can send requests to.
// It starts listening but does not actually start handling incomming requests.
func NewServer(addr, dir string) (*Server, error) {
	server, err := transport.Serve(addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		trServer: server,
		dbs:      kadiyadb.LoadAll(dir),
	}

	s.sync = function.NewGroup(s.Sync)
	return s, nil
}

// Start starts processing incomming requests
func (s *Server) Start() error {
	go func() {
		for _ = range time.Tick(syncPeriod * time.Millisecond) {
			s.sync.Flush()
		}
	}()

	for {
		conn, err := s.trServer.Accept()
		if err != nil {
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn *transport.Conn) {
	tr := transport.New(conn)

	for {
		data, id, msgType, err := tr.ReceiveBatch()
		if err != nil {
			break
		}

		go s.handleMessage(tr, data, id, msgType)
	}

	if err := conn.Close(); err != nil {
		fmt.Println(err)
	}
}

func (s *Server) handleMessage(tr *transport.Transport, data [][]byte, id uint32, msgType uint8) {
	var err error

	switch msgType {
	case MsgTypeTrack:
		err = tr.SendBatch(s.handleTrack(data), id, MsgTypeTrack)
	case MsgTypeFetch:
		err = tr.SendBatch(s.handleFetch(data), id, MsgTypeFetch)
	}

	if err != nil {
		fmt.Println(err)
	}
}

func (s *Server) handleTrack(trackBatch [][]byte) (resBatch [][]byte) {
	resBytes := make([][]byte, len(trackBatch))
	req := &protocol.ReqTrack{}
	res := &protocol.ResTrack{}

	setResponse := func(i int, res *protocol.ResTrack, errmsg string) {
		res.Error = errmsg
		buf, err := res.Marshal()
		if err != nil {
			fmt.Println(err)
		} else {
			resBytes[i] = buf
		}
	}

	for i, trackData := range trackBatch {
		// Reset structs for reuse
		req.Fields = req.Fields[:0]

		if err := req.Unmarshal(trackData); err != nil {
			setResponse(i, res, ErrCorruptMsg.Error())
			continue
		}

		db, ok := s.dbs[req.Database]
		if !ok {
			setResponse(i, res, ErrUnknownDB.Error())
			continue
		}

		if err := db.Track(req.Time, req.Fields, req.Total, req.Count); err != nil {
			setResponse(i, res, err.Error())
			continue
		}

		setResponse(i, res, "")
	}

	s.sync.Run()
	return resBytes
}

func (s *Server) handleFetch(fetchBatch [][]byte) (resBatch [][]byte) {
	resBytes := make([][]byte, len(fetchBatch))
	req := &protocol.ReqFetch{}
	res := &protocol.ResFetch{}

	setResponse := func(i int, res *protocol.ResFetch, errmsg string, chunks []*protocol.Chunk) {
		res.Error = errmsg
		res.Chunks = chunks
		buf, err := res.Marshal()
		if err != nil {
			fmt.Println(err)
		} else {
			resBytes[i] = buf
		}
	}

	for i, fetchData := range fetchBatch {
		// Reset structs for reuse
		req.Fields = req.Fields[:0]
		res.Chunks = res.Chunks[:0]

		err := req.Unmarshal(fetchData)
		if err != nil {
			setResponse(i, res, ErrCorruptMsg.Error(), nil)
			continue
		}

		db, ok := s.dbs[req.Database]
		if !ok {
			setResponse(i, res, ErrUnknownDB.Error(), nil)
			return
		}

		db.Fetch(req.From, req.To, req.Fields, func(chunks []*protocol.Chunk, err error) {
			if err != nil {
				setResponse(i, res, err.Error(), nil)
				return
			}

			setResponse(i, res, "", chunks)
		})
	}

	return resBytes
}

// Sync syncs every database in the server
func (s *Server) Sync() {
	for dbname, db := range s.dbs {
		if err := db.Sync(); err != nil {
			fmt.Printf("Error while syncing database (name: %s) %s\n", dbname, err)
		}
	}
}

// ListDatabases returns a list of names of loaded databases
func (s *Server) ListDatabases() (list []string) {
	list = make([]string, 0, len(s.dbs))

	for db := range s.dbs {
		list = append(list, db)
	}

	return list
}
