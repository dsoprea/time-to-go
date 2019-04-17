package timetogo

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

func TestIndex_GetWithTimestamp_Hit(t *testing.T) {
	raw, footers, _ := WriteTestMultiseriesStream()

	r := bytes.NewReader(raw)

	index, err := NewIndex(r)
	log.PanicIf(err)

	queryTimestamp := time.Date(2016, 10, 1, 12, 34, 57, 0, time.UTC)

	matched, err := index.GetWithTimestamp(queryTimestamp)
	log.PanicIf(err)

	if len(matched) != 1 {
		t.Fatalf("Exactly one was not found but should have been: (%d)", len(matched))
	} else if matched[0].Uuid() != footers[0].Uuid() {
		t.Fatalf("The first series was not returned: [%s] != [%s]", matched[0].Uuid(), footers[0].Uuid())
	}
}

func TestIndex_GetWithTimestamp_Miss_TooEarly(t *testing.T) {
	raw, _, _ := WriteTestMultiseriesStream()

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
	raw, footers, _ := WriteTestMultiseriesStream()

	r := bytes.NewReader(raw)

	index, err := NewIndex(r)
	log.PanicIf(err)

	queryTimestamp := time.Date(2016, 10, 1, 12, 35, 8, 0, time.UTC)

	matched, err := index.GetWithTimestamp(queryTimestamp)
	log.PanicIf(err)

	if len(matched) != 2 {
		t.Fatalf("Exactly two were not found but should have been: (%d)", len(matched))
	} else if matched[0].Uuid() != footers[0].Uuid() {
		t.Fatalf("The first series was not correct: [%s] != [%s]", matched[0].Uuid(), footers[0].Uuid())
	} else if matched[1].Uuid() != footers[1].Uuid() {
		t.Fatalf("The second series was not correct: [%s] != [%s]", matched[1].Uuid(), footers[1].Uuid())
	}
}

func TestIndex_GetWithTimestamp_Miss_TooLate(t *testing.T) {
	raw, _, _ := WriteTestMultiseriesStream()

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

func ExampleIndex_GetWithTimestamp() {
	b := rifs.NewSeekableBuffer()

	// Stage stream.

	sb := NewStreamBuilder(b)
	series := AddTestSeries(sb)

	_, err := sb.Finish()
	log.PanicIf(err)

	raw := b.Bytes()

	for i, series := range series {
		fmt.Printf("Test series (%d): %s [%v, %v]\n", i, series.Uuid(), series.HeadRecordTime(), series.TailRecordTime())
	}

	fmt.Printf("\n")

	// Parse stream.

	r := bytes.NewReader(raw)

	index, err := NewIndex(r)
	log.PanicIf(err)

	queryTimestamp := time.Date(2016, 10, 1, 12, 34, 57, 0, time.UTC)

	fmt.Printf("Query: %v\n", queryTimestamp)
	fmt.Printf("\n")

	matched, err := index.GetWithTimestamp(queryTimestamp)
	log.PanicIf(err)

	for _, matchedSeries := range matched {
		fmt.Printf("MATCHED: %s\n", matchedSeries.Uuid())
	}

	// Output:
	// Test series (0): d095abf5-126e-48a7-8974-885de92bd964 [2016-10-01 12:34:56 +0000 UTC, 2016-10-01 12:35:16 +0000 UTC]
	// Test series (1): 8a4ba0c4-0a0d-442f-8256-1d61adb16abc [2016-10-01 12:35:06 +0000 UTC, 2016-10-01 12:35:26 +0000 UTC]
	//
	// Query: 2016-10-01 12:34:57 +0000 UTC
	//
	// MATCHED: d095abf5-126e-48a7-8974-885de92bd964
}
