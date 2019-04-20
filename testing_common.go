package timetogo

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

var (
	// TestTimeSeriesData is test data.
	TestTimeSeriesData = []byte("some time series data")

	// TestTimeSeriesData2 is test data.
	TestTimeSeriesData2 = []byte("X some time series data 2 X")
)

// DumpBytes prints raw bytes.
func DumpBytes(description string, rs io.ReadSeeker, position int64, count int, requireAll bool) {
	originalPosition, err := rs.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	_, err = rs.Seek(position, os.SEEK_SET)
	log.PanicIf(err)

	collected := make([]byte, count)
	ptr := collected

	j := count
	for j > 0 {
		n, err := rs.Read(ptr)
		if err == io.EOF {
			break
		}

		ptr = ptr[n:]
		j -= n
	}

	_, err = rs.Seek(originalPosition, os.SEEK_SET)
	log.PanicIf(err)

	if requireAll == true && len(collected) < count {
		log.Panicf("not enough bytes available")
	}

	fmt.Printf("DUMP(%s):", description)
	for i := 0; i < count; i++ {
		fmt.Printf(" %02x", collected[i])
	}

	fmt.Printf("\n")
}

// WriteTestMultiseriesStream creates a stream with multiple test-series and
// validates that it looks okay before returning.
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

// AddTestSeries will append two test series to the given builder.
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
	// testable examples.
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
