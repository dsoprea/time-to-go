package timetogo

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func WriteTestMultiseriesStream() (raw []byte, footers []*SeriesFooter1, sb *StreamBuilder) {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb = NewStreamBuilder(b)
	sb.sw.SetStructureLogging(true)

	// Add first series.

	// Make sure the timestamp now matches the same one later by using UTC.
	headRecordTime := time.Date(2016, 10, 1, 12, 34, 56, 0, time.UTC)
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 20)

	sourceSha1 := []byte{
		11,
		22,
		33,
	}

	originalSeriesFooter1 := NewSeriesFooter1(
		headRecordTime,
		tailRecordTime,
		uint64(len(TestTimeSeriesData)),
		22,
		"some_filename",
		sourceSha1)

	dataReader1 := bytes.NewBuffer(TestTimeSeriesData)

	err := sb.AddSeries(dataReader1, originalSeriesFooter1)
	log.PanicIf(err)

	seriesSize1 := sb.nextOffset

	if seriesSize1 != 179 {
		log.Panicf("first series size not correct: (%d)", seriesSize1)
	}

	// Add second series.

	sourceSha12 := []byte{
		44,
		55,
		66,
	}

	originalSeriesFooter2 := NewSeriesFooter1(
		headRecordTime.Add(time.Second*10),
		tailRecordTime.Add(time.Second*10),
		uint64(len(TestTimeSeriesData2)),
		33,
		"some_filename2",
		sourceSha12)

	dataReader2 := bytes.NewBuffer(TestTimeSeriesData2)

	err = sb.AddSeries(dataReader2, originalSeriesFooter2)
	log.PanicIf(err)

	seriesSize2 := sb.nextOffset

	if seriesSize2 != 364 {
		log.Panicf("second series size not correct: (%d)", seriesSize2)
	}

	// Finish stream.

	totalSize, err := sb.Finish()
	log.PanicIf(err)

	raw = b.Bytes()

	if len(raw) != 634 {
		log.Panicf("stream data is not the right size: (%d)", len(raw))
	}

	if totalSize != len(raw) {
		log.Panicf("stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", totalSize, len(raw))
	}

	// Now, verify that all of the boundaries are in the right places (at least theoretically).
	structure := sb.Structure()
	milestones := structure.MilestonesWithFilter(string(MtBoundaryMarker), -1)

	if len(milestones) != 3 {
		log.Panicf("exactly three boundary markers not written: %v", milestones)
	}

	boundaryOffset := milestones[0].Offset
	if boundaryOffset != 178 {
		log.Panicf("first boundary offset not correct: (%d)", boundaryOffset)
	}

	boundaryOffset = milestones[1].Offset
	if boundaryOffset != 363 {
		log.Panicf("second boundary offset not correct: (%d)", boundaryOffset)
	}

	series := []*SeriesFooter1{
		originalSeriesFooter1,
		originalSeriesFooter2,
	}

	return raw, series, sb
}

func TestNewIterator_Iterate(t *testing.T) {
	raw, originalFooters, _ := WriteTestMultiseriesStream()

	r := bytes.NewReader(raw)
	sr := NewStreamReader(r)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	if it.Count() != 2 {
		t.Fatalf("The stream didn't see exactly two series: (%d)", it.Count())
	}

	if it.Current() != 1 {
		t.Fatalf("The current series is not (0): (%d)", it.Current())
	}

	// Read first series.

	b2 := &bytes.Buffer{}

	seriesFooterInterface2, checksumOk, err := it.Iterate(b2)
	log.PanicIf(err)

	if checksumOk != true {
		t.Fatalf("Checksum does not match.")
	}

	seriesData2 := b2.Bytes()

	if it.Current() != 0 {
		t.Fatalf("The current series is not (1): (%d)", it.Current())
	}

	indexInfo2 := it.SeriesInfo(1)

	if indexInfo2.Uuid() != originalFooters[1].Uuid() {
		t.Fatalf("Series 2 UUID in the index isn't correct: [%s] != [%s]", indexInfo2.Uuid(), originalFooters[1].Uuid())
	}

	headIndex := indexInfo2.HeadRecordTime()
	headFooter := seriesFooterInterface2.HeadRecordTime()

	if headIndex != headFooter {
		t.Fatalf("Series 2 head record-time in the index doesn't match: [%s] != [%s]", headIndex, headFooter)
	}

	tailIndex := indexInfo2.TailRecordTime()
	tailFooter := seriesFooterInterface2.TailRecordTime()

	if tailIndex != tailFooter {
		t.Fatalf("Series 2 tail record-time in the index doesn't match: [%s] != [%s]", tailIndex, tailFooter)
	}

	recoveredSeriesFooter2 := seriesFooterInterface2.(*SeriesFooter1)

	originalFooters[1].dataFnv1aChecksum = 0xba7ac887

	if reflect.DeepEqual(recoveredSeriesFooter2, originalFooters[1]) != true {
		t.Fatalf("Series footer 2 was not recovered correctly.")
	} else if bytes.Compare(seriesData2, TestTimeSeriesData2) != 0 {
		t.Fatalf("Series data 2 was not recovered correctly.")
	}

	// Read second series.

	b1 := &bytes.Buffer{}

	seriesFooterInterface1, checksumOk, err := it.Iterate(b1)
	log.PanicIf(err)

	if checksumOk != true {
		t.Fatalf("Checksum does not match.")
	}

	seriesData1 := b1.Bytes()

	if it.Current() != -1 {
		t.Fatalf("The current series is not (-1): (%d)", it.Current())
	}

	indexInfo1 := it.SeriesInfo(0)

	if indexInfo1.Uuid() != originalFooters[0].Uuid() {
		t.Fatalf("Series 1 UUID in the index isn't correct: [%s] != [%s]", indexInfo1.Uuid(), originalFooters[0].Uuid())
	}

	headIndex = indexInfo1.HeadRecordTime()
	headFooter = seriesFooterInterface1.HeadRecordTime()

	if headIndex != headFooter {
		t.Fatalf("Series 1 head record-time in the index doesn't match: [%s] != [%s]", headIndex, headFooter)
	}

	tailIndex = indexInfo1.TailRecordTime()
	tailFooter = seriesFooterInterface1.TailRecordTime()

	if tailIndex != tailFooter {
		t.Fatalf("Series 1 tail record-time in the index doesn't match: [%s] != [%s]", tailIndex, tailFooter)
	}

	recoveredSeriesFooter1 := seriesFooterInterface1.(*SeriesFooter1)

	originalFooters[0].dataFnv1aChecksum = 0xefd515f5

	if reflect.DeepEqual(recoveredSeriesFooter1, originalFooters[0]) != true {
		t.Fatalf("Series footer 1 was not recovered correctly.")
	} else if bytes.Compare(seriesData1, TestTimeSeriesData) != 0 {
		t.Fatalf("Series data 1 was not recovered correctly.")
	}

	// Check EOF.

	_, _, err = it.Iterate(nil)
	if err != io.EOF {
		t.Fatalf("Expected EOF.")
	}

	if it.Current() != -1 {
		t.Fatalf("The current series is not (-1): (%d)", it.Current())
	}
}

func AddTestSeries(sb *StreamBuilder) (footers []*SeriesFooter1) {
	// Add first series.

	// Make sure the timestamp now matches the same one later by using UTC.
	headRecordTime := time.Date(2016, 10, 1, 12, 34, 56, 0, time.UTC)
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 20)

	sourceSha1 := []byte{
		11,
		22,
		33,
	}

	originalSeriesFooter1 := NewSeriesFooter1(
		headRecordTime,
		tailRecordTime,
		uint64(len(TestTimeSeriesData)),
		22,
		"some_filename",
		sourceSha1)

	// Force a specific UUID so we know the exact output in support of the
	originalSeriesFooter1.uuid = "d095abf5-126e-48a7-8974-885de92bd964"

	dataReader1 := bytes.NewBuffer(TestTimeSeriesData)

	err := sb.AddSeries(dataReader1, originalSeriesFooter1)
	log.PanicIf(err)

	// Add second series.

	sourceSha12 := []byte{
		44,
		55,
		66,
	}

	originalSeriesFooter2 := NewSeriesFooter1(
		headRecordTime.Add(time.Second*10),
		tailRecordTime.Add(time.Second*10),
		uint64(len(TestTimeSeriesData2)),
		33,
		"some_filename2",
		sourceSha12)

	// Force a specific UUID so we know the exact output in support of the
	// testable examples.
	originalSeriesFooter2.uuid = "8a4ba0c4-0a0d-442f-8256-1d61adb16abc"

	dataReader2 := bytes.NewBuffer(TestTimeSeriesData2)

	err = sb.AddSeries(dataReader2, originalSeriesFooter2)
	log.PanicIf(err)

	series := []*SeriesFooter1{
		originalSeriesFooter1,
		originalSeriesFooter2,
	}

	return series
}

// ExampleIterator_Iterate shows how to parse and step through stream data.
// Remember that we'll start from the end and step backwards.
//
// See ExampleStreamReader_ReadSeriesWithIndexedInfo for an example of how to
// perform random or ordered reads of series within a stream (instead of having
// to step backward through all of them, in order).
func ExampleIterator_Iterate() {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb := NewStreamBuilder(b)

	series := AddTestSeries(sb)

	for i, seriesFooter := range series {
		fmt.Printf("Test series (%d): [%s]\n", i, seriesFooter.Uuid())
	}

	fmt.Printf("\n")

	_, err := sb.Finish()
	log.PanicIf(err)

	raw := b.Bytes()

	// Open the stream.

	r := bytes.NewReader(raw)

	sr := NewStreamReader(r)
	sr.SetStructureLogging(true)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	// Very cheap calls. Keep in mind that we will actually iterate through
	// these in reverse order, below.
	fmt.Printf("Number of series recorded in stream footer: (%d)\n", it.Count())

	sisi := it.SeriesInfo(0)
	fmt.Printf("Indexed series 0: %s\n", sisi.Uuid())

	sisi = it.SeriesInfo(1)
	fmt.Printf("Indexed series 1: %s\n", sisi.Uuid())

	fmt.Printf("\n")

	// Read first encountered series.

	seriesNumber := it.Current()

	seriesData := new(bytes.Buffer)

	seriesFooter, checksumOk, err := it.Iterate(seriesData)
	log.PanicIf(err)

	if checksumOk != true {
		log.Panicf("first encountered checksum does not match")
	}

	fmt.Printf("Encountered series (%d): %s\n", seriesNumber, seriesFooter.Uuid())

	// This is the original time-series' blob. It's the caller's responsibility
	// to encode it and decode it.
	fmt.Printf("Series (%d) data: %s\n", seriesNumber, string(seriesData.Bytes()))

	// Read second encountered series.

	seriesNumber = it.Current()

	seriesData = new(bytes.Buffer)

	seriesFooter, checksumOk, err = it.Iterate(seriesData)
	log.PanicIf(err)

	if checksumOk != true {
		log.Panicf("second encountered checksum does not match")
	}

	fmt.Printf("Encountered series (%d): %s\n", seriesNumber, seriesFooter.Uuid())

	// This is the original time-series' blob. It's the caller's responsibility
	// to encode it and decode it.
	fmt.Printf("Series (%d) data: %s\n", seriesNumber, string(seriesData.Bytes()))

	// Check EOF.

	_, _, err = it.Iterate(nil)
	if err != io.EOF {
		log.Panicf("expected EOF")
	}

	fmt.Printf("\n")

	// Show that the structure loggingrepresents the offsets in reverse order
	// (the order that they're visited). Note that certain milestones will
	// include more than one entry. Some milestones can't be completely
	// interpreted/applied until more information is read. So, we'll log those
	// milestones as soon as they're encountered as well as when we have more
	// information about them.
	sr.Structure().Dump()

	// Output:
	// Test series (0): [d095abf5-126e-48a7-8974-885de92bd964]
	// Test series (1): [8a4ba0c4-0a0d-442f-8256-1d61adb16abc]
	//
	// Number of series recorded in stream footer: (2)
	// Indexed series 0: d095abf5-126e-48a7-8974-885de92bd964
	// Indexed series 1: 8a4ba0c4-0a0d-442f-8256-1d61adb16abc
	//
	// Encountered series (1): 8a4ba0c4-0a0d-442f-8256-1d61adb16abc
	// Series (1) data: X some time series data 2 X
	// Encountered series (0): d095abf5-126e-48a7-8974-885de92bd964
	// Series (0) data: some time series data
	//
	// ================
	// Stream Structure
	// ================
	//
	// OFF 633      MT boundary_marker                 SCOPE stream   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 628      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 364      MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM
	//              MT stream_footer_decoded           SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(2)>
	// OFF 363      MT boundary_marker                 SCOPE series   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 358      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 206      MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT series_footer_head_byte         SCOPE series   UUID                                           COMM
	//              MT series_footer_decoded           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 179      MT series_data_head_byte           SCOPE series   UUID 8a4ba0c4-0a0d-442f-8256-1d61adb16abc      COMM
	// OFF 178      MT boundary_marker                 SCOPE series   UUID                                           COMM
	//              MT boundary_marker                 SCOPE misc     UUID                                           COMM
	// OFF 173      MT shadow_footer_head_byte         SCOPE misc     UUID                                           COMM
	// OFF 21       MT footer_head_byte                SCOPE misc     UUID                                           COMM
	//              MT series_footer_head_byte         SCOPE series   UUID                                           COMM
	//              MT series_footer_decoded           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
	// OFF 0        MT series_data_head_byte           SCOPE series   UUID d095abf5-126e-48a7-8974-885de92bd964      COMM
}
