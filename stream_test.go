package timetogo

import (
    "bytes"
    "fmt"
    "io"
    "os"
    "reflect"
    "testing"

    "github.com/dsoprea/go-logging"
)

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

func TestStream__Protocol1(t *testing.T) {
    b := new(bytes.Buffer)

    // Stage stream.

    cw := NewCountingWriter(b)
    sw := NewStreamWriter(cw)

    testSeriesFooter, seriesSize := WriteTestSeriesFooter1(cw, sw)

    if cw.Position() != seriesSize {
        t.Fatalf("Current file pointer is not on the boundary marker after the stream footer: (%d) != (%d)", cw.Position(), seriesSize)
    }

    testStreamFooterSeries, streamSize := WriteTestStreamFooter1(sw)

    if cw.Position() != seriesSize+streamSize {
        t.Fatalf("Current file pointer is not on the boundary marker after the series footer: (%d) != (%d)", cw.Position(), seriesSize+streamSize)
    }

    raw := b.Bytes()

    if len(raw) != 273 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    } else if seriesSize+streamSize != len(raw) {
        t.Fatalf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", seriesSize, streamSize)
    }

    // Validate stream.

    r := bytes.NewReader(raw)
    sr := NewStreamReader(r)

    // Put us on the trailing NUL byte.
    _, err := r.Seek(-1, os.SEEK_END)
    log.PanicIf(err)

    // Vaidate stream footer.

    sf, nextBoundaryOffset, err := sr.readStreamFooter()
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

    sfRecoveredInterface, dataOffset, nextBoundaryOffset, err := sr.readSeriesFooter()
    log.PanicIf(err)

    if nextBoundaryOffset != -1 {
        t.Fatalf("Next boundary after series not correct: (%d)", nextBoundaryOffset)
    }

    seriesFooter := sfRecoveredInterface.(*SeriesFooter1)

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
