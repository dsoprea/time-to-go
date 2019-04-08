package timetogo

import (
    "bytes"
    "os"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func TestStreamWriter__stream_write_and_read(t *testing.T) {
    // Make sure the timestamp now matches thesame one later.
    now := time.Now().UTC()
    now = now.Add(-time.Nanosecond * time.Duration(now.Nanosecond()))

    isis1 := NewStreamIndexedSequenceInfo1(
        now.Add(time.Hour*1),
        now.Add(time.Hour*2),
        "aa",
        11)

    isis2 := NewStreamIndexedSequenceInfo1(
        now.Add(time.Hour*3),
        now.Add(time.Hour*4),
        "bb",
        22)

    sequences := []*StreamIndexedSequenceInfo1{
        isis1,
        isis2,
    }

    b := new(bytes.Buffer)
    sw := NewStreamWriter(b)

    err := sw.writeStreamFooter1(sequences)
    log.PanicIf(err)

    raw := b.Bytes()

    if len(raw) != 142 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    }

    r := bytes.NewReader(raw)

    // Put us on the trailing NUL byte.
    _, err = r.Seek(-1, os.SEEK_END)
    log.PanicIf(err)

    sr := NewStreamReader(r)

    sf, err := sr.readStreamFooter()
    log.PanicIf(err)

    series := sf.Series()
    if len(series) != 2 {
        t.Fatalf("We did not find exactly two series: (%d)", len(series))
    }

    // TODO(dustin): Why don't these fail since we will lose the nanoseconds that the original structs had.

    if series[0] == isis1 {
        t.Fatalf("First series is not correct.")
    } else if series[1] == isis2 {
        t.Fatalf("Second series is not correct.")
    }
}
