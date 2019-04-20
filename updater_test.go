package timetogo

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func TestUpdater_AddSeries_NoChange(t *testing.T) {
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
	} else if totalSize != 554 {
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
	} else if seriesFooter2.UpdatedTime() != series[1].UpdatedTime() {
		t.Fatalf("First encountered series has an updated timestamp but shouldn't: [%v] != [%v]", seriesFooter2.UpdatedTime(), series[1].UpdatedTime())
	}

	seriesFooter1, _, err := it.Iterate(nil)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != series[0].Uuid() {
		t.Fatalf("Second encountered series is not correct.")
	} else if seriesFooter1.UpdatedTime() != series[0].UpdatedTime() {
		t.Fatalf("First encountered series has an updated timestamp but shouldn't: [%v] != [%v]", seriesFooter1.UpdatedTime(), series[0].UpdatedTime())
	}
}

type SeriesDataTestGenerator struct {
	data map[string]io.Reader
}

func (sdtg *SeriesDataTestGenerator) WriteData(w io.Writer, sf SeriesFooter) (n int, err error) {
	rc, found := sdtg.data[sf.Uuid()]
	if found == false {
		log.Panicf("footer data not found: %s", sf)
	}

	count, err := io.Copy(w, rc)
	log.PanicIf(err)

	return int(count), nil
}

func TestUpdater_AddSeries_AddNew(t *testing.T) {
	raw, series, _ := WriteTestMultiseriesStream()

	// Update.

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
		sourceSha13)

	dataReader3 := bytes.NewBuffer(TestTimeSeriesData2)

	sdtg := &SeriesDataTestGenerator{
		data: map[string]io.Reader{
			sf3.Uuid(): dataReader3,
		},
	}

	rws := rifs.NewSeekableBufferWithBytes(raw)

	updater := NewUpdater(rws, sdtg)
	updater.SetStructureLogging(true)

	series0UpdatedTime := series[0].UpdatedTime()
	series1UpdatedTime := series[1].UpdatedTime()
	series2UpdatedTime := sf3.UpdatedTime()

	updater.AddSeries(series[0])
	updater.AddSeries(series[1])
	updater.AddSeries(sf3)

	if series[0].UpdatedTime() != series0UpdatedTime {
		t.Fatalf("Series 0 update time changed but shouldn't have.")
	} else if series[1].UpdatedTime() != series1UpdatedTime {
		t.Fatalf("Series 1 update time changed but shouldn't have.")
	} else if sf3.UpdatedTime() != series2UpdatedTime {
		t.Fatalf("Series 2 update time changed but shouldn't have.")
	}

	time.Sleep(time.Second * 1)

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Skips: 2,
		Adds:  1,
	}

	if series[0].UpdatedTime() != series0UpdatedTime {
		t.Fatalf("Series 0 update time changed but shouldn't have.")
	} else if series[1].UpdatedTime() != series1UpdatedTime {
		t.Fatalf("Series 1 update time changed but shouldn't have.")
	} else if sf3.UpdatedTime().After(series2UpdatedTime) != true {
		t.Fatalf("Series 2 update time SHOULD HAVE been bumped but wasn't: [%v] != [%v]", sf3.UpdatedTime(), series2UpdatedTime)
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 827 {
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

	// Read back.

	r := bytes.NewReader(finalRaw)
	sr := NewStreamReader(r)

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

// TestUpdater_AddSeries__CopyForward drops the first series and induces
// `Updater` to copy the data from the back of the stream to the front of the
// stream.
func TestUpdater_AddSeries__DropOne(t *testing.T) {
	raw, series, _ := WriteTestMultiseriesStream()

	// Update.

	rws := rifs.NewSeekableBufferWithBytes(raw)
	updater := NewUpdater(rws, nil)

	// We add the second one instead of the first so we can guarantee a non-
	// trivial operation.
	updater.AddSeries(series[0])

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Skips: 1,
		Drops: 1,
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 297 {
		t.Fatalf("Total stream size not correct: (%d)", totalSize)
	}

	finalRaw := rws.Bytes()

	// Read back.

	r := bytes.NewReader(finalRaw)
	sr := NewStreamReader(r)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 1 {
		t.Fatalf("The stream doesn't have exactly one series: (%d)", it.Count())
	}

	b := new(bytes.Buffer)

	seriesFooter1, _, err := it.Iterate(b)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != series[0].Uuid() {
		t.Fatalf("First encountered series is not correct.")
	}

	dataBytes := b.Bytes()

	if bytes.Compare(dataBytes, TestTimeSeriesData) != 0 {
		t.Fatalf("Series 1 data not correct:\nACTUAL: %v\nEXPECTED: %v", dataBytes, TestTimeSeriesData)
	}
}

// TestUpdater_AddSeries__CopyForward drops the first series and induces
// `Updater` to copy the data from the back of the stream to the front of the
// stream.
func TestUpdater_AddSeries__CopyForward(t *testing.T) {
	raw, series, _ := WriteTestMultiseriesStream()

	// Update.

	rws := rifs.NewSeekableBufferWithBytes(raw)
	updater := NewUpdater(rws, nil)

	// We add the second one instead of the first so we can guarantee a non-
	// trivial operation.
	updater.AddSeries(series[1])

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Skips: 1,
		Drops: 1,
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 303 {
		t.Fatalf("Total stream size not correct: (%d)", totalSize)
	}

	finalRaw := rws.Bytes()

	// Read back.

	r := bytes.NewReader(finalRaw)
	sr := NewStreamReader(r)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 1 {
		t.Fatalf("The stream doesn't have exactly one series: (%d)", it.Count())
	}

	b := new(bytes.Buffer)

	seriesFooter1, _, err := it.Iterate(b)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != series[1].Uuid() {
		t.Fatalf("First encountered series is not correct.")
	}

	dataBytes := b.Bytes()

	if bytes.Compare(dataBytes, TestTimeSeriesData2) != 0 {
		t.Fatalf("Series 1 data not correct:\nACTUAL: %v\nEXPECTED: %v", dataBytes, TestTimeSeriesData)
	}
}

func ExampleUpdater_AddSeries() {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb := NewStreamBuilder(b)
	sb.SetStructureLogging(true)

	series := AddTestSeries(sb)

	_, err := sb.Finish()
	log.PanicIf(err)

	fmt.Printf("\n")
	fmt.Printf("Original:\n")
	fmt.Printf("\n")

	sb.Structure().Dump()

	raw := b.Bytes()

	// Update the stream with a new series.

	sourceSha13 := []byte{
		77,
		88,
		99,
	}

	now := time.Now()

	series3 := NewSeriesFooter1(
		now.Add(time.Second*20),
		now.Add(time.Second*30),
		uint64(len(TestTimeSeriesData2)),
		33,
		sourceSha13)

	// Force a specific UUID so we know the exact output in support of the
	// testable examples.
	series3.uuid = "9a0e2d13-d14f-4a57-b43c-24bd3de6581e"

	dataReader3 := bytes.NewBuffer(TestTimeSeriesData2)

	sdtg := &SeriesDataTestGenerator{
		data: map[string]io.Reader{
			series3.Uuid(): dataReader3,
		},
	}

	rws := rifs.NewSeekableBufferWithBytes(raw)
	updater := NewUpdater(rws, sdtg)

	updater.AddSeries(series[0])
	updater.AddSeries(series[1])
	updater.AddSeries(series3)

	_, _, err = updater.Write()
	log.PanicIf(err)

	finalRaw := rws.Bytes()

	// Read the new stream.

	r := bytes.NewReader(finalRaw)

	sr := NewStreamReader(r)
	sr.SetStructureLogging(true)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	for {
		_, _, err := it.Iterate(nil)
		if err == io.EOF {
			break
		}
	}

	fmt.Printf("Updated:\n")
	fmt.Printf("\n")

	sr.Structure().Dump()

	// Output:
	// Original:

	// ================
	// Stream Structure
	// ================

	// OFF 0        MT series_data_head_byte           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 21       MT series_footer_head_byte         SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 149      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 154      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 155      MT series_data_head_byte           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 182      MT series_footer_head_byte         SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 310      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 315      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 316      MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(2)>
	// OFF 516      MT shadow_footer_head_byte         SCOPE stream   UUID                                           COMM
	// OFF 521      MT boundary_marker                 SCOPE stream   UUID                                           COMM

	// Updated:

	// ================
	// Stream Structure
	// ================

	// OFF 778      MT boundary_marker                 SCOPE stream   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 773      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 477      MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM
	//              MT stream_footer_decoded           SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(3)>
	// OFF 476      MT boundary_marker                 SCOPE series   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 471      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 343      MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT series_footer_head_byte         SCOPE series   UUID                                           COMM
	//              MT series_footer_decoded           SCOPE series   UUID 9a0e2d13-d14f-4a57-b43c-24bd3de6581e      COMM
	// OFF 316      MT series_data_head_byte           SCOPE series   UUID 9a0e2d13-d14f-4a57-b43c-24bd3de6581e      COMM
	// OFF 315      MT boundary_marker                 SCOPE series   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 310      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 182      MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT series_footer_head_byte         SCOPE series   UUID                                           COMM
	//              MT series_footer_decoded           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 155      MT series_data_head_byte           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 154      MT boundary_marker                 SCOPE series   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 149      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 21       MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT series_footer_head_byte         SCOPE series   UUID                                           COMM
	//              MT series_footer_decoded           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 0        MT series_data_head_byte           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
}

func TestUpdater_AddSeries_FromEmpty(t *testing.T) {
	sourceSha13 := []byte{
		77,
		88,
		99,
	}

	now := time.Now()

	sf1 := NewSeriesFooter1(
		now.Add(time.Second*10),
		now.Add(time.Second*20),
		uint64(len(TestTimeSeriesData2)),
		33,
		sourceSha13)

	dataReader1 := bytes.NewBuffer(TestTimeSeriesData2)

	sdtg := &SeriesDataTestGenerator{
		data: map[string]io.Reader{
			sf1.Uuid(): dataReader1,
		},
	}

	raw := []byte{}
	rws := rifs.NewSeekableBufferWithBytes(raw)

	updater := NewUpdater(rws, sdtg)
	updater.AddSeries(sf1)

	totalSize, stats, err := updater.Write()
	log.PanicIf(err)

	expectedStats := UpdateStats{
		Adds: 1,
	}

	if stats != expectedStats {
		t.Fatalf("Stats not correct: %s", stats)
	} else if totalSize != 303 {
		t.Fatalf("Total stream size not correct: (%d)", totalSize)
	}

	// Validate.

	sr := NewStreamReader(rws)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 1 {
		t.Fatalf("The stream doesn't have exactly two series: (%d)", it.Count())
	}

	seriesFooter1, _, err := it.Iterate(nil)
	log.PanicIf(err)

	if seriesFooter1.Uuid() != sf1.Uuid() {
		t.Fatalf("First encountered series is not correct.")
	}
}
