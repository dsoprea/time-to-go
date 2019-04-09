package timetogo

import (
    "bytes"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func TestIndex_GetWithTimestamp_Hit(t *testing.T) {
    raw, footers := WriteTestMultiseriesStream()

    r := bytes.NewReader(raw)

    index, err := NewIndex(r)
    log.PanicIf(err)

    queryTimestamp := time.Date(2016, 10, 1, 12, 34, 57, 0, time.UTC)

    matched, err := index.GetWithTimestamp(queryTimestamp)
    log.PanicIf(err)

    if len(matched) != 1 {
        t.Fatalf("Exactly one was not found but should have been: (%d)", len(matched))
    } else if matched[0].OriginalFilename() != footers[0].OriginalFilename() {
        t.Fatalf("The first series was not returned: [%s] != [%s]", matched[0].OriginalFilename(), footers[0].OriginalFilename())
    }
}

func TestIndex_GetWithTimestamp_Miss_TooEarly(t *testing.T) {
    raw, _ := WriteTestMultiseriesStream()

    r := bytes.NewReader(raw)

    index, err := NewIndex(r)
    log.PanicIf(err)

    queryTimestamp := time.Date(2016, 10, 1, 12, 34, 55, 0, time.UTC)

    matched, err := index.GetWithTimestamp(queryTimestamp)
    log.PanicIf(err)

    if len(matched) != 0 {
        t.Fatalf("Expected no matches: (%d)", len(matched))
    }
}

func TestIndex_GetWithTimestamp_Hit_Intersection(t *testing.T) {
    raw, footers := WriteTestMultiseriesStream()

    r := bytes.NewReader(raw)

    index, err := NewIndex(r)
    log.PanicIf(err)

    queryTimestamp := time.Date(2016, 10, 1, 12, 35, 8, 0, time.UTC)

    matched, err := index.GetWithTimestamp(queryTimestamp)
    log.PanicIf(err)

    if len(matched) != 2 {
        t.Fatalf("Exactly two were not found but should have been: (%d)", len(matched))
    } else if matched[0].OriginalFilename() != footers[0].OriginalFilename() {
        t.Fatalf("The first series was not correct: [%s] != [%s]", matched[0].OriginalFilename(), footers[0].OriginalFilename())
    } else if matched[1].OriginalFilename() != footers[1].OriginalFilename() {
        t.Fatalf("The second series was not correct: [%s] != [%s]", matched[1].OriginalFilename(), footers[1].OriginalFilename())
    }
}

func TestIndex_GetWithTimestamp_Miss_TooLate(t *testing.T) {
    raw, _ := WriteTestMultiseriesStream()

    r := bytes.NewReader(raw)

    index, err := NewIndex(r)
    log.PanicIf(err)

    queryTimestamp := time.Date(2016, 10, 1, 12, 36, 55, 0, time.UTC)

    matched, err := index.GetWithTimestamp(queryTimestamp)
    log.PanicIf(err)

    if len(matched) != 0 {
        t.Fatalf("Expected no matches: (%d)", len(matched))
    }
}
