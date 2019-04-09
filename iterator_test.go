package timetogo

import (
    "bytes"
    "io"
    "reflect"
    "testing"
    "time"

    "github.com/dsoprea/go-logging"
)

func WriteTestMultiseriesStream() (raw []byte, footers []*SeriesFooter1) {
    b := new(bytes.Buffer)

    // Stage stream.

    sb := NewStreamBuilder(b)

    // Add first series.

    // Make sure the timestamp now matches the same one later by using UTC.
    headRecordTime := time.Date(2016, 10, 1, 12, 34, 56, 0, time.UTC)
    headRecordTime = headRecordTime.Add(-time.Nanosecond * time.Duration(headRecordTime.Nanosecond()))

    tailRecordTime := headRecordTime.Add(time.Second * 20)

    sourceSha1 := []byte{
        11,
        22,
        33,
    }

    dataFnv1aChecksum := uint32(1234)

    originalSeriesFooter1 := NewSeriesFooter1(
        headRecordTime,
        tailRecordTime,
        uint64(len(TestTimeSeriesData)),
        22,
        "some_filename",
        sourceSha1,
        dataFnv1aChecksum)

    err := sb.AddSeries(TestTimeSeriesData, originalSeriesFooter1)
    log.PanicIf(err)

    seriesSize1 := sb.nextOffset

    if seriesSize1 != 179 {
        log.Panicf("First series size not correct: (%d)", seriesSize1)
    }

    // Add second series.

    sourceSha12 := []byte{
        44,
        55,
        66,
    }

    dataFnv1aChecksum2 := uint32(1234)

    originalSeriesFooter2 := NewSeriesFooter1(
        headRecordTime.Add(time.Second*10),
        tailRecordTime.Add(time.Second*10),
        uint64(len(TestTimeSeriesData2)),
        33,
        "some_filename2",
        sourceSha12,
        dataFnv1aChecksum2)

    err = sb.AddSeries(TestTimeSeriesData2, originalSeriesFooter2)
    log.PanicIf(err)

    seriesSize2 := sb.nextOffset

    if seriesSize2 != 364 {
        log.Panicf("Second series size not correct: (%d)", seriesSize2)
    }

    // Finish stream.

    totalSize, err := sb.Finish()
    log.PanicIf(err)

    raw = b.Bytes()

    if len(raw) != 538 {
        log.Panicf("stream data is not the right size: (%d)", len(raw))
    }

    if totalSize != uint64(len(raw)) {
        log.Panicf("Stream components are not the right size: SERIES-SIZE=(%d) STREAM-SIZE=(%d)", totalSize, len(raw))
    }

    series := []*SeriesFooter1{
        originalSeriesFooter1,
        originalSeriesFooter2,
    }

    return raw, series
}

func TestNewIterator_Iterate(t *testing.T) {
    raw, originalFooters := WriteTestMultiseriesStream()

    r := bytes.NewReader(raw)

    it, err := NewIterator(r)
    log.PanicIf(err)

    if it.Count() != 2 {
        t.Fatalf("The stream didn't see exactly two series: (%d)", it.Count())
    }

    if it.Current() != 1 {
        t.Fatalf("The current series is not (0): (%d)", it.Current())
    }

    // Read first series.

    seriesFooterInterface2, seriesData2, err := it.Iterate()
    log.PanicIf(err)

    if it.Current() != 0 {
        t.Fatalf("The current series is not (1): (%d)", it.Current())
    }

    indexInfo2 := it.SeriesInfo(1)

    if indexInfo2.OriginalFilename() != originalFooters[1].OriginalFilename() {
        t.Fatalf("Series 2 filename in the index doesn't match: [%s]", indexInfo2.OriginalFilename())
    }

    headIndex := indexInfo2.HeadRecordTime()
    headFooter := seriesFooterInterface2.HeadRecordTime()

    if headIndex != headFooter {
        t.Fatalf("Series 2 head record-time in the index doesn't match: [%s] != [%s]", headIndex, headFooter)
    }

    tailIndex := indexInfo2.TailRecordTime()
    tailFooter := seriesFooterInterface2.TailRecordTime()

    if tailIndex != tailFooter {
        t.Fatalf("Series 2 tail record-time in the index doesn't match: [%s] != [%s]", tailIndex, tailFooter)
    }

    recoveredSeriesFooter2 := seriesFooterInterface2.(*SeriesFooter1)

    if reflect.DeepEqual(recoveredSeriesFooter2, originalFooters[1]) != true {
        t.Fatalf("Series footer 2 was not recovered correctly.")
    } else if bytes.Compare(seriesData2, TestTimeSeriesData2) != 0 {
        t.Fatalf("Series data 2 was not recovered correctly.")
    }

    // Read second series.

    seriesFooterInterface1, seriesData1, err := it.Iterate()
    log.PanicIf(err)

    if it.Current() != -1 {
        t.Fatalf("The current series is not (-1): (%d)", it.Current())
    }

    indexInfo1 := it.SeriesInfo(0)

    if indexInfo1.OriginalFilename() != originalFooters[0].OriginalFilename() {
        t.Fatalf("Series 1 filename in the index doesn't match: [%s]", indexInfo1.OriginalFilename())
    }

    headIndex = indexInfo1.HeadRecordTime()
    headFooter = seriesFooterInterface1.HeadRecordTime()

    if headIndex != headFooter {
        t.Fatalf("Series 1 head record-time in the index doesn't match: [%s] != [%s]", headIndex, headFooter)
    }

    tailIndex = indexInfo1.TailRecordTime()
    tailFooter = seriesFooterInterface1.TailRecordTime()

    if tailIndex != tailFooter {
        t.Fatalf("Series 1 tail record-time in the index doesn't match: [%s] != [%s]", tailIndex, tailFooter)
    }

    recoveredSeriesFooter1 := seriesFooterInterface1.(*SeriesFooter1)

    if reflect.DeepEqual(recoveredSeriesFooter1, originalFooters[0]) != true {
        t.Fatalf("Series footer 1 was not recovered correctly.")
    } else if bytes.Compare(seriesData1, TestTimeSeriesData) != 0 {
        t.Fatalf("Series data 1 was not recovered correctly.")
    }

    // Check EOF.

    _, _, err = it.Iterate()
    if err != io.EOF {
        t.Fatalf("Expected EOF.")
    }

    if it.Current() != -1 {
        t.Fatalf("The current series is not (-1): (%d)", it.Current())
    }
}
