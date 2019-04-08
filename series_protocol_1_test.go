package timetogo

import (
    "bytes"
    "os"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func TestStreamWriter__series_write_and_read(t *testing.T) {
    b := new(bytes.Buffer)

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

    sfOriginal :=
        NewSeriesFooter1(
            headRecordTime,
            tailRecordTime,
            11,
            22,
            "some_filename",
            sourceSha1,
            dataFnv1aChecksum)

    sw := NewStreamWriter(b)

    err := sw.writeSeriesFooter1(sfOriginal)
    log.PanicIf(err)

    raw := b.Bytes()

    if len(raw) != 110 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    }

    r := bytes.NewReader(raw)

    // Put us on the trailing NUL byte.
    _, err = r.Seek(-1, os.SEEK_END)
    log.PanicIf(err)

    sr := NewStreamReader(r)

    sfRecoveredInterface, err := sr.readSeriesFooter()
    log.PanicIf(err)

    sfRecovered := sfRecoveredInterface.(*SeriesFooter1)

    if reflect.DeepEqual(sfRecovered, sfOriginal) != true {
        t.Fatalf("Recovered record is not correct:\nACTUAL:\n%v\nEXPECTED:\n%v", sfRecovered, sfOriginal)
    }
}
