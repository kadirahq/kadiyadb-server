package block

import (
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"github.com/kadirahq/go-tools/segmmap"
)

var (
	tmpdir  = "/tmp/test-segmmap/"
	tmpfile = tmpdir + "file_"
)

func setup(t *testing.T) {
	if err := os.RemoveAll(tmpdir); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(tmpdir, 0777); err != nil {
		t.Fatal(err)
	}
}

func clear(t *testing.T) {
	time.Sleep(time.Second) // segmmap may add one more segfile before removing
	// directory, which gives "directory not empty". So wait a second for it.

	if err := os.RemoveAll(tmpdir); err != nil {
		t.Fatal(err)
	}
}

func TestFromByteArr(t *testing.T) {
	dummyLen, dummyCap := 120, 1200

	dummySlice := make([]byte, dummyLen, dummyCap)

	pSlice := fromByteSlice(dummySlice)

	if len(pSlice) != dummyLen/pointsz || cap(pSlice) != dummyCap/pointsz {
		t.Fatal("Pointer slice have a wrong lenth or capacity.")
	}

	dummyPoint := Point{
		Total: 3.141592,
		Count: 10,
	}

	(pSlice)[1] = dummyPoint

	bits := binary.LittleEndian.Uint64(dummySlice[16:24])
	total := math.Float64frombits(bits)

	if total != dummyPoint.Total {
		t.Fatal("Total is not set properly.")
	}
}

func TestReadRecords(t *testing.T) {
	setup(t)
	defer clear(t)

	dummySegmapSize := int64(96 * 5)

	m, err := segmmap.NewMap(tmpfile, dummySegmapSize)

	if err != nil {
		t.Fatal(err)
	}

	testBlock := Block{
		recs: [][]Point{},
		mmap: m,
		rbs:  96,
	}

	m.Load(int64(0))

	bits := math.Float64bits(3.14)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)

	// Write at 2nd point of first record
	if n, err := m.WriteAt(bytes, 16); err != nil {
		t.Fatal(err)
	} else if n != 8 {
		t.Fatal("Wrong size")
	}

	testBlock.readRecords()

	if int64(len(testBlock.recs)) != dummySegmapSize/testBlock.rbs {
		t.Fatal("Wrong length in Block Records")
	}

	record := testBlock.recs[0] // first record
	point := record[1]          // second point

	if point.Total != 3.14 {
		t.Fatal("Wrong data in Block Record")
	}
}

func TestNewBlock(t *testing.T) {
	setup(t)
	defer clear(t)

	block, err := New(tmpdir, 2)

	if err != nil {
		t.Fatal(err)
	}

	if len(block.recs) != 0 {
		t.Fatal("Wrong length")
	}
}

func TestAdd(t *testing.T) {
	setup(t)
	defer clear(t)

	testRecordSize := int64(100)

	block, err := New(tmpdir, testRecordSize)
	if err != nil {
		t.Fatal(err)
	}

	if len(block.recs) != 0 {
		t.Fatal("Wrong length")
	}

	err = block.Track(0, 0, 123.456, 5)
	if err != nil {
		t.Fatal(err)
	}

	err = block.Track(7, 3, 123.456, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Find expected value for Record length
	testRecordByteSize := (testRecordSize * pointsz)
	expectedRLen := segsz / testRecordByteSize

	if int64(len(block.recs)) != expectedRLen {
		t.Fatal("Wrong length. Expected:", expectedRLen,
			"Got:", len(block.recs))
	}

	// It should be able to write to an index larger than seg size.
	err = block.Track(expectedRLen, 0, 123.456, 5)

	if err != nil {
		t.Fatal(err)
	}

	if int64(len(block.recs)) != 2*expectedRLen {
		t.Fatal("Wrong length")
	}

	if block.recs[0][0].Total != 123.456 ||
		block.recs[7][3].Total != 123.456 ||
		block.recs[expectedRLen][0].Total != 123.456 {
		t.Fatal("Total not set correctly")
	}

	if block.recs[0][0].Count != 5 ||
		block.recs[7][3].Count != 5 ||
		block.recs[expectedRLen][0].Count != 5 {
		t.Fatal("Count not set correctly")
	}
}
