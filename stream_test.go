package timetogo

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func TestStream__Protocol1(t *testing.T) {
	// Stage stream.

	sb := rifs.NewSeekableBuffer()
	sw := NewStreamWriter(sb)

	testSeriesFooter, seriesSize := WriteTestSeriesFooter1(sb, sw)

	position, err := sb.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	if int(position) != seriesSize {
		t.Fatalf("Current file pointer is not on the boundary marker after the stream footer: (%d) != (%d)", position, seriesSize)
	}

	testStreamFooterSeries, streamSize := WriteTestStreamFooter1(sw)

	position, err = sb.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	if int(position) != seriesSize+streamSize {
		t.Fatalf("Current file pointer is not on the boundary marker after the series footer: (%d) != (%d)", position, seriesSize+streamSize)
	}

	raw := sb.Bytes()

	if len(raw) != 353 {
		t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
	} else if seriesSize+streamSize != len(raw) {
		t.Fatalf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", seriesSize, streamSize)
	}

	// Validate stream.

	r := bytes.NewReader(raw)
	sr := NewStreamReader(r)

	// Put us on the trailing NUL byte.
	err = sr.Reset()
	log.PanicIf(err)

	// Vaidate stream footer.

	sf, nextBoundaryOffset, _, err := sr.readStreamFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != int64(seriesSize)-1 {
		t.Fatalf("Next-boundary offset after the stream-footer is not correct: (%d)", nextBoundaryOffset)
	}

	streamFooterSeries := sf.Series()
	if len(streamFooterSeries) != 2 {
		t.Fatalf("We did not find exactly two series: (%d)", len(streamFooterSeries))
	}

	if streamFooterSeries[0] == testStreamFooterSeries[0] {
		t.Fatalf("First series is not correct.")
	} else if streamFooterSeries[1] == testStreamFooterSeries[1] {
		t.Fatalf("Second series is not correct.")
	}

	// Validate series footer.

	sfRecoveredInterface, dataOffset, nextBoundaryOffset, _, err := sr.readSeriesFooter()
	log.PanicIf(err)

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next boundary after series not correct: (%d)", nextBoundaryOffset)
	}

	seriesFooter := sfRecoveredInterface.(*SeriesFooter1)

	testSeriesFooter.dataFnv1aChecksum = 305419896

	if reflect.DeepEqual(seriesFooter, testSeriesFooter) != true {
		t.Fatalf("Recovered record is not correct:\nACTUAL:\n%v\nEXPECTED:\n%v", seriesFooter, testSeriesFooter)
	}

	// Validate series data.

	_, err = r.Seek(dataOffset, os.SEEK_SET)
	log.PanicIf(err)

	recoveredData := make([]byte, len(TestTimeSeriesData))
	_, err = io.ReadFull(r, recoveredData)
	log.PanicIf(err)

	if reflect.DeepEqual(recoveredData, TestTimeSeriesData) != true {
		t.Fatalf("Time-series data was not recovered correctly:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredData, TestTimeSeriesData)
	}

	// Validate that there are no more series.

	if nextBoundaryOffset != -1 {
		t.Fatalf("Next-boundary offset after the series-data is not correct: (%d)", nextBoundaryOffset)
	}
}
