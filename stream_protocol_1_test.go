package timetogo

import (
    "bytes"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func WriteTestStreamFooter1(sw *StreamWriter) ([]StreamIndexedSequenceInfo, int) {
    // Make sure the timestamp now matches thesame one later.
    now := time.Now().UTC()
    now = now.Add(-time.Nanosecond * time.Duration(now.Nanosecond()))

    isis1 := NewStreamIndexedSequenceInfo1(
        "uuid1",
        now.Add(time.Hour*1),
        now.Add(time.Hour*2),
        "aa",
        11)

    isis2 := NewStreamIndexedSequenceInfo1(
        "uuid2",
        now.Add(time.Hour*3),
        now.Add(time.Hour*4),
        "bb",
        22)

    series := []StreamIndexedSequenceInfo{
        isis1,
        isis2,
    }

    size, err := sw.writeStreamFooter(series)
    log.PanicIf(err)

    if size != 174 {
        log.Panicf("Stream footer is not the right size: (%d)", size)
    }

    return series, size
}

func TestStreamWriter__StreamWriteAndRead(t *testing.T) {
    b := new(bytes.Buffer)
    sw := NewStreamWriter(b)

    testSeries, _ := WriteTestStreamFooter1(sw)

    raw := b.Bytes()

    if len(raw) != 174 {
        t.Fatalf("Encoded data is not the right size: (%d)", len(raw))
    }

    r := bytes.NewReader(raw)
    sr := NewStreamReader(r)

    // Put us on the trailing NUL byte.
    err := sr.Reset()
    log.PanicIf(err)

    sf, nextBoundaryOffset, _, err := sr.readStreamFooter()
    log.PanicIf(err)

    if nextBoundaryOffset != -1 {
        t.Fatalf("Expected next-boundary offset to be just before the beginning of the file: (%d)", nextBoundaryOffset)
    }

    series := sf.Series()
    if len(series) != 2 {
        t.Fatalf("We did not find exactly two series: (%d)", len(series))
    }

    if series[0] == testSeries[0] {
        t.Fatalf("First series is not correct.")
    } else if series[1] == testSeries[1] {
        t.Fatalf("Second series is not correct.")
    }
}
