package timetogo

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"io/ioutil"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func TestUpdater_AddSeries_NoChange(t *testing.T) {
	defer func() {
		if state := recover(); state != nil {
			err := log.Wrap(state.(error))
			log.PrintError(err)
			t.Fatalf("Test failed.")
		}
	}()

	raw, series, _ := WriteTestMultiseriesStream()

	rws := rifs.NewSeekableBufferWithBytes(raw)

	updater := NewUpdater(rws, nil)
	updater.AddSeries(series[0])
	updater.AddSeries(series[1])

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Skips: 2,
		Adds:  0,
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 634 {
		t.Fatalf("Total stream size not correct: (%d)", totalSize)
	}

	finalRaw := rws.Bytes()

	r := bytes.NewReader(finalRaw)
	sr := NewStreamReader(r)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 2 {
		t.Fatalf("The stream doesn't have exactly two series: (%d)", it.Count())
	}

	seriesFooter2, _, err := it.Iterate(nil)
	log.PanicIf(err)

	if seriesFooter2.Uuid() != series[1].Uuid() {
		t.Fatalf("First encountered series is not correct.")
	}

	seriesFooter1, _, err := it.Iterate(nil)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != series[0].Uuid() {
		t.Fatalf("Second encountered series is not correct.")
	}
}

type SeriesDataTestGenerator struct {
	data map[string]io.ReadCloser
}

func (sdtg *SeriesDataTestGenerator) GetSerializeTimeSeriesData(seriesFooter SeriesFooter) (rc io.ReadCloser, err error) {
	rc, found := sdtg.data[seriesFooter.Uuid()]
	if found == false {
		log.Panicf("footer data not found: %s", seriesFooter)
	}

	return rc, nil
}

func TestUpdater_AddSeries_AddNew(t *testing.T) {
	defer func() {
		if state := recover(); state != nil {
			err := log.Wrap(state.(error))
			log.PrintError(err)
			t.Fatalf("Test failed.")
		}
	}()

	raw, series, _ := WriteTestMultiseriesStream()

	sourceSha13 := []byte{
		77,
		88,
		99,
	}

	now := time.Now()

	sf3 := NewSeriesFooter1(
		now.Add(time.Second*10),
		now.Add(time.Second*20),
		uint64(len(TestTimeSeriesData2)),
		33,
		"some_filename3",
		sourceSha13)

	dataReader3 := bytes.NewBuffer(TestTimeSeriesData2)
	drc := ioutil.NopCloser(dataReader3)

	sdtg := &SeriesDataTestGenerator{
		data: map[string]io.ReadCloser{
			sf3.Uuid(): drc,
		},
	}

	rws := rifs.NewSeekableBufferWithBytes(raw)

	updater := NewUpdater(rws, sdtg)
	updater.SetStructureLogging(true)

	updater.AddSeries(series[0])
	updater.AddSeries(series[1])
	updater.AddSeries(sf3)

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Skips: 2,
		Adds:  1,
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 939 {
		t.Fatalf("Total stream size not correct: (%d)", totalSize)
	}

	finalRaw := rws.Bytes()

	structure := updater.Structure()
	boundaries := structure.MilestonesWithFilter(string(MtBoundaryMarker), -1)

	boundaryCount := len(boundaries)
	if boundaryCount != 4 {
		t.Fatalf("The wrong number of boundaries were found: (%d)", boundaryCount)
	}

	lastBoundaryOffset := int(boundaries[boundaryCount-1].Offset)
	if lastBoundaryOffset != len(finalRaw)-1 {
		t.Fatalf("Last boundary is not correct: (%d) != (%d)", lastBoundaryOffset, len(finalRaw)-1)
	}

	// Verify that all of the reported boundaries are NUL bytes.

	misses := 0
	for i, ssoi := range boundaries {
		c := finalRaw[ssoi.Offset]
		if c != 0 {
			fmt.Printf("boundary (%d) is not a NUL: %s\n", i, ssoi)
			misses++
		}
	}

	if misses > 0 {
		log.Panicf("one or more of the reported boundaries was not NUL")
	}

	r := bytes.NewReader(finalRaw)
	sr := NewStreamReader(r)

	// TODO(dustin): !! Debugging.
	sr.SetStructureLogging(true)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 3 {
		t.Fatalf("The stream doesn't have exactly three series: (%d)", it.Count())
	}

	b := new(bytes.Buffer)

	seriesFooter3, _, err := it.Iterate(b)
	log.PanicIf(err)

	dataBytes := b.Bytes()

	if seriesFooter3.Uuid() != sf3.Uuid() {
		t.Fatalf("First encountered series is not correct.")
	}

	if bytes.Compare(dataBytes, TestTimeSeriesData2) != 0 {
		t.Fatalf("Series 3 data not correct:\nACTUAL: %v\nEXPECTED: %v", dataBytes, TestTimeSeriesData2)
	}

	b = new(bytes.Buffer)

	seriesFooter2, _, err := it.Iterate(b)
	log.PanicIf(err)

	if seriesFooter2.Uuid() != series[1].Uuid() {
		t.Fatalf("Second encountered series is not correct.")
	}

	dataBytes = b.Bytes()

	if bytes.Compare(dataBytes, TestTimeSeriesData2) != 0 {
		t.Fatalf("Series 2 data not correct:\nACTUAL: %v\nEXPECTED: %v", dataBytes, TestTimeSeriesData2)
	}

	b = new(bytes.Buffer)

	seriesFooter1, _, err := it.Iterate(b)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != series[0].Uuid() {
		t.Fatalf("Third encountered series is not correct.")
	}

	dataBytes = b.Bytes()

	if bytes.Compare(dataBytes, TestTimeSeriesData) != 0 {
		t.Fatalf("Series 1 data not correct:\nACTUAL: %v\nEXPECTED: %v", dataBytes, TestTimeSeriesData)
	}
}

// TODO(dustin): !! Add a test where there *are* changes.
