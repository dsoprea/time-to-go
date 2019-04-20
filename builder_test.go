package timetogo

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func WriteTestStreamWithDatasource() (raw []byte, originalSeriesFooter *SeriesFooter1, seriesSize int64) {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb := NewStreamBuilder(b)

	// Make sure the timestamp now matches thesame one later.
	headRecordTime := time.Now().UTC()
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 10)

	sourceSha1 := []byte{
		11,
		22,
		33,
	}

	originalSeriesFooter = NewSeriesFooter1(
		headRecordTime,
		tailRecordTime,
		uint64(len(TestTimeSeriesData)),
		22,
		"some_filename",
		sourceSha1)

	dataReader := bytes.NewBuffer(TestTimeSeriesData)
	sddw := NewSeriesDataDatasourceWrapperFromReader(dataReader)

	err := sb.AddSeries(sddw, originalSeriesFooter)
	log.PanicIf(err)

	seriesSize = sb.nextOffset

	totalSize, err := sb.Finish()
	log.PanicIf(err)

	raw = b.Bytes()

	if len(raw) != 337 {
		log.Panicf("encoded data is not the right size: (%d)", len(raw))
	} else if totalSize != len(raw) {
		log.Panicf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", totalSize, len(raw))
	}

	return raw, originalSeriesFooter, seriesSize
}

func WriteTestStreamWithReader() (raw []byte, originalSeriesFooter *SeriesFooter1, seriesSize int64) {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb := NewStreamBuilder(b)

	// Make sure the timestamp now matches thesame one later.
	headRecordTime := time.Now().UTC()
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 10)

	sourceSha1 := []byte{
		11,
		22,
		33,
	}

	originalSeriesFooter = NewSeriesFooter1(
		headRecordTime,
		tailRecordTime,
		uint64(len(TestTimeSeriesData)),
		22,
		"some_filename",
		sourceSha1)

	dataReader := bytes.NewBuffer(TestTimeSeriesData)

	err := sb.AddSeries(dataReader, originalSeriesFooter)
	log.PanicIf(err)

	seriesSize = sb.nextOffset

	totalSize, err := sb.Finish()
	log.PanicIf(err)

	raw = b.Bytes()

	if len(raw) != 337 {
		log.Panicf("encoded data is not the right size: (%d)", len(raw))
	} else if totalSize != len(raw) {
		log.Panicf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", totalSize, len(raw))
	}

	return raw, originalSeriesFooter, seriesSize
}

func TestBuilder_Finish_Reader(t *testing.T) {
	raw, originalSeriesFooter, seriesSize := WriteTestStreamWithReader()

	// Validate stream.

	r := bytes.NewReader(raw)
	sr := NewStreamReader(r)

	// Put us on the trailing NUL byte.
	err := sr.Reset()
	log.PanicIf(err)

	// Vaidate stream footer.

	streamFooter, nextBoundaryOffset, _, err := sr.readStreamFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != int64(seriesSize)-1 {
		t.Fatalf("Next-boundary offset after the stream-footer is not correct: (%d)", nextBoundaryOffset)
	}

	streamFooterSeries := streamFooter.Series()
	if len(streamFooterSeries) != 1 {
		t.Fatalf("We did not find exactly two series: (%d)", len(streamFooterSeries))
	}

	// Validate series footer.

	recoveredSeriesFooterInterface, dataOffset, nextBoundaryOffset, _, err := sr.readSeriesFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next boundary after series not correct: (%d)", nextBoundaryOffset)
	}

	recoveredSeriesFooter := recoveredSeriesFooterInterface.(*SeriesFooter1)

	originalSeriesFooter.dataFnv1aChecksum = 4023719413

	if reflect.DeepEqual(recoveredSeriesFooter, originalSeriesFooter) != true {
		t.Fatalf("Recovered record is not correct:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredSeriesFooter, originalSeriesFooter)
	}

	// Validate series data.

	_, err = r.Seek(dataOffset, os.SEEK_SET)
	log.PanicIf(err)

	recoveredData := make([]byte, len(TestTimeSeriesData))
	_, err = io.ReadFull(r, recoveredData)
	log.PanicIf(err)

	if bytes.Compare(recoveredData, TestTimeSeriesData) != 0 {
		t.Fatalf("Time-series data was not recovered correctly:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredData, TestTimeSeriesData)
	}

	// Validate that there are no more series.

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next-boundary offset after the series-data is not correct: (%d)", nextBoundaryOffset)
	}
}

func TestBuilder_Finish_Datasource(t *testing.T) {
	raw, originalSeriesFooter, seriesSize := WriteTestStreamWithDatasource()

	// Validate stream.

	r := bytes.NewReader(raw)
	sr := NewStreamReader(r)

	// Put us on the trailing NUL byte.
	err := sr.Reset()
	log.PanicIf(err)

	// Vaidate stream footer.

	streamFooter, nextBoundaryOffset, _, err := sr.readStreamFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != int64(seriesSize)-1 {
		t.Fatalf("Next-boundary offset after the stream-footer is not correct: (%d)", nextBoundaryOffset)
	}

	streamFooterSeries := streamFooter.Series()
	if len(streamFooterSeries) != 1 {
		t.Fatalf("We did not find exactly two series: (%d)", len(streamFooterSeries))
	}

	// Validate series footer.

	recoveredSeriesFooterInterface, dataOffset, nextBoundaryOffset, _, err := sr.readSeriesFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next boundary after series not correct: (%d)", nextBoundaryOffset)
	}

	recoveredSeriesFooter := recoveredSeriesFooterInterface.(*SeriesFooter1)

	originalSeriesFooter.dataFnv1aChecksum = 4023719413

	if reflect.DeepEqual(recoveredSeriesFooter, originalSeriesFooter) != true {
		t.Fatalf("Recovered record is not correct:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredSeriesFooter, originalSeriesFooter)
	}

	// Validate series data.

	_, err = r.Seek(dataOffset, os.SEEK_SET)
	log.PanicIf(err)

	recoveredData := make([]byte, len(TestTimeSeriesData))
	_, err = io.ReadFull(r, recoveredData)
	log.PanicIf(err)

	if bytes.Compare(recoveredData, TestTimeSeriesData) != 0 {
		t.Fatalf("Time-series data was not recovered correctly:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredData, TestTimeSeriesData)
	}

	// Validate that there are no more series.

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next-boundary offset after the series-data is not correct: (%d)", nextBoundaryOffset)
	}
}

// ExampleStreamBuilder_AddSeries_Reader shows us to build and write a stream.
// You can create new series or use existing series (from iterating an existing
// stream using `Iterate`). The `Updater` type may be used for more intelligent
// updates.
//
// This example uses a `io.Reader` to get the time-series data to be written.
func ExampleStreamBuilder_AddSeries_Reader() {
	b := rifs.NewSeekableBuffer()

	sb := NewStreamBuilder(b)

	// Enable structure-tracking so we can print a table of the structure later.
	sb.SetStructureLogging(true)

	// Make sure the timestamp now matches the same one later by using UTC.
	headRecordTime := time.Date(2016, 10, 1, 12, 34, 56, 0, time.UTC)
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 20)

	// Add first series.

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

	// Force a specific UUID so we know what to expect below.
	originalSeriesFooter1.uuid = "ca38f9e3-bdea-4bc8-9a8a-22681ea815b0"

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

	// Force a specific UUID so we know what to expect below.
	originalSeriesFooter2.uuid = "1616bda4-c570-4d05-a346-674e4c051460"

	dataReader2 := bytes.NewBuffer(TestTimeSeriesData2)

	err = sb.AddSeries(dataReader2, originalSeriesFooter2)
	log.PanicIf(err)

	// Finish stream.

	_, err = sb.Finish()
	log.PanicIf(err)

	// Print final structure.
	sb.Structure().Dump()

	// Output:
	// ================
	// Stream Structure
	// ================
	//
	// OFF 0        MT series_data_head_byte           SCOPE series   UUID ca38f9e3-bdea-4bc8-9a8a-22681ea815b0      COMM
	// OFF 21       MT series_footer_head_byte         SCOPE series   UUID ca38f9e3-bdea-4bc8-9a8a-22681ea815b0      COMM
	// OFF 173      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 178      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 179      MT series_data_head_byte           SCOPE series   UUID 1616bda4-c570-4d05-a346-674e4c051460      COMM
	// OFF 206      MT series_footer_head_byte         SCOPE series   UUID 1616bda4-c570-4d05-a346-674e4c051460      COMM
	// OFF 358      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 363      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 364      MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(2)>
	// OFF 628      MT shadow_footer_head_byte         SCOPE stream   UUID                                           COMM
	// OFF 633      MT boundary_marker                 SCOPE stream   UUID                                           COMM
}

// ExampleStreamBuilder_AddSeries_Datasource shows us to build and write a
// stream. You can create new series or use existing series (from iterating an
// existing stream using `Iterate`). The `Updater` type may be used for more
// intelligent updates.
//
// This example uses a `SeriesDataDatasource` to manage the writing process,
// where we provide the writer and delegate the writing task to the caller.
func ExampleStreamBuilder_AddSeries_Datasource() {
	b := rifs.NewSeekableBuffer()

	sb := NewStreamBuilder(b)

	// Enable structure-tracking so we can print a table of the structure later.
	sb.SetStructureLogging(true)

	// Make sure the timestamp now matches the same one later by using UTC.
	headRecordTime := time.Date(2016, 10, 1, 12, 34, 56, 0, time.UTC)
	headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

	tailRecordTime := headRecordTime.Add(time.Second * 20)

	// Add first series.

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

	// Force a specific UUID so we know what to expect below.
	originalSeriesFooter1.uuid = "ca38f9e3-bdea-4bc8-9a8a-22681ea815b0"

	dataReader1 := bytes.NewBuffer(TestTimeSeriesData)
	sddw := NewSeriesDataDatasourceWrapperFromReader(dataReader1)

	err := sb.AddSeries(sddw, originalSeriesFooter1)
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

	// Force a specific UUID so we know what to expect below.
	originalSeriesFooter2.uuid = "1616bda4-c570-4d05-a346-674e4c051460"

	dataReader2 := bytes.NewBuffer(TestTimeSeriesData2)

	err = sb.AddSeries(dataReader2, originalSeriesFooter2)
	log.PanicIf(err)

	// Finish stream.

	_, err = sb.Finish()
	log.PanicIf(err)

	// Print final structure.
	sb.Structure().Dump()

	// Output:
	// ================
	// Stream Structure
	// ================
	//
	// OFF 0        MT series_data_head_byte           SCOPE series   UUID ca38f9e3-bdea-4bc8-9a8a-22681ea815b0      COMM
	// OFF 21       MT series_footer_head_byte         SCOPE series   UUID ca38f9e3-bdea-4bc8-9a8a-22681ea815b0      COMM
	// OFF 173      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 178      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 179      MT series_data_head_byte           SCOPE series   UUID 1616bda4-c570-4d05-a346-674e4c051460      COMM
	// OFF 206      MT series_footer_head_byte         SCOPE series   UUID 1616bda4-c570-4d05-a346-674e4c051460      COMM
	// OFF 358      MT shadow_footer_head_byte         SCOPE series   UUID                                           COMM
	// OFF 363      MT boundary_marker                 SCOPE series   UUID                                           COMM
	// OFF 364      MT stream_footer_head_byte         SCOPE stream   UUID                                           COMM Stream: StreamFooter1<COUNT=(2)>
	// OFF 628      MT shadow_footer_head_byte         SCOPE stream   UUID                                           COMM
	// OFF 633      MT boundary_marker                 SCOPE stream   UUID                                           COMM
}
