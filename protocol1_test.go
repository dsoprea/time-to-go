package timetogo

import (
    "bytes"
    "os"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func TestStreamWriter__write_and_read(t *testing.T) {
    b := new(bytes.Buffer)

    headRecordEpoch := uint64(time.Now().Unix())
    tailRecordEpoch := headRecordEpoch + 10

    sourceSha1 := []byte{
        11,
        22,
        33,
    }

    dataFnv1aChecksum := uint32(1234)

    sfOriginal :=
        NewSeriesFooter1(
            headRecordEpoch,
            tailRecordEpoch,
            11,
            22,
            "some_filename",
            sourceSha1,
            dataFnv1aChecksum)

    sw := NewStreamWriter(b)

    err := sw.writeFooter1(sfOriginal)
    log.PanicIf(err)

    raw := b.Bytes()

    if len(raw) != 109 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    }

    r := bytes.NewReader(raw)

    // Put us on the trailing NUL byte.
    _, err = r.Seek(-1, os.SEEK_END)
    log.PanicIf(err)

    sr := NewStreamReader(r)

    sfRecoveredInt, err := sr.readFooter()
    log.PanicIf(err)

    sfRecovered := sfRecoveredInt.(*SeriesFooter1)

    if reflect.DeepEqual(sfRecovered, sfOriginal) != true {
        t.Fatalf("Recovered record is not correct.")
    }
}
