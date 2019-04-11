package timetogo

import (
    "bytes"
    "io"
    "os"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
    "github.com/google/uuid"
)

func WriteTestSeriesFooter1(w io.Writer, sw *StreamWriter) (sfOriginal *SeriesFooter1, size int) {
    // Write time-series data.
    dataSize, err := w.Write(TestTimeSeriesData)
    log.PanicIf(err)

    // Make sure the timestamp now matches thesame one later.
    headRecordTime := time.Now().UTC()
    headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

    tailRecordTime := headRecordTime.Add(time.Second * 10)

    sourceSha1 := []byte{
        11,
        22,
        33,
    }

    sfOriginal =
        NewSeriesFooter1(
            headRecordTime,
            tailRecordTime,
            uint64(len(TestTimeSeriesData)),
            22,
            "some_filename",
            sourceSha1)

    // Make sure we actually have a UUID.
    _, err = uuid.Parse(sfOriginal.Uuid())
    log.PanicIf(err)

    footerSize, err := sw.writeSeriesFooter1(sfOriginal, 0x12345678)
    log.PanicIf(err)

    size = dataSize + footerSize

    if size != 179 {
        log.Panicf("Series footer was not the correct size: (%d)", size)
    }

    return sfOriginal, size
}

func TestStreamWriter__SeriesWriteAndRead(t *testing.T) {
    b := new(bytes.Buffer)
    sw := NewStreamWriter(b)

    sfOriginal, _ := WriteTestSeriesFooter1(b, sw)

    raw := b.Bytes()

    if len(raw) != 179 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    }

    r := bytes.NewReader(raw)
    sr := NewStreamReader(r)

    // Put us on the trailing NUL byte.
    err := sr.Reset()
    log.PanicIf(err)

    sfRecoveredInterface, dataOffset, nextBoundaryOffset, _, err := sr.readSeriesFooter()
    log.PanicIf(err)

    sfRecovered := sfRecoveredInterface.(*SeriesFooter1)
    sfOriginal.dataFnv1aChecksum = 305419896

    if reflect.DeepEqual(sfRecovered, sfOriginal) != true {
        t.Fatalf("Recovered record is not correct:\nACTUAL:\n%v\nEXPECTED:\n%v", sfRecovered, sfOriginal)
    }

    _, err = r.Seek(dataOffset, os.SEEK_SET)
    log.PanicIf(err)

    recoveredData := make([]byte, len(TestTimeSeriesData))
    _, err = io.ReadFull(r, recoveredData)
    log.PanicIf(err)

    if reflect.DeepEqual(recoveredData, TestTimeSeriesData) != true {
        t.Fatalf("Time-series data was not recovered correctly:\nACTUAL:\n%v\nEXPECTED:\n%v", recoveredData, TestTimeSeriesData)
    }

    if nextBoundaryOffset != -1 {
        t.Fatalf("Next boundary offset expected to be just before beginning of file: (%d)", nextBoundaryOffset)
    }
}
