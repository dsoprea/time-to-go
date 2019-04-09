package timetogo

import (
    "bytes"
    "io"
    "os"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func WriteTestStream() (raw []byte, originalSeriesFooter *SeriesFooter1, seriesSize int64) {
    b := new(bytes.Buffer)

    // Stage stream.

    cw := NewCountingWriter(b)
    sb := NewStreamBuilder(cw)

    // Make sure the timestamp now matches thesame one later.
    headRecordTime := time.Now().UTC()
    headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

    tailRecordTime := headRecordTime.Add(time.Second * 10)

    sourceSha1 := []byte{
        11,
        22,
        33,
    }

    dataFnv1aChecksum := uint32(1234)

    originalSeriesFooter = NewSeriesFooter1(
        headRecordTime,
        tailRecordTime,
        uint64(len(TestTimeSeriesData)),
        22,
        "some_filename",
        sourceSha1,
        dataFnv1aChecksum)

    err := sb.AddSeries(TestTimeSeriesData, originalSeriesFooter)
    log.PanicIf(err)

    seriesSize = sb.nextOffset

    totalSize, err := sb.Finish()
    log.PanicIf(err)

    raw = b.Bytes()

    if len(raw) != 289 {
        log.Panicf("encoded data is not the right size: (%d)", len(raw))
    } else if totalSize != uint64(len(raw)) {
        log.Panicf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", totalSize, len(raw))
    }

    return raw, originalSeriesFooter, seriesSize
}

func TestBuilder_Finish(t *testing.T) {
    raw, originalSeriesFooter, seriesSize := WriteTestStream()

    // Validate stream.

    r := bytes.NewReader(raw)
    sr := NewStreamReader(r)

    // Put us on the trailing NUL byte.
    _, err := r.Seek(-1, os.SEEK_END)
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
