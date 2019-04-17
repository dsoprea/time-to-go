package timetogo

import (
    "bytes"
    "fmt"

    "github.com/dsoprea/go-logging"
    "github.com/randomingenuity/go-utility/filesystem"
)

// ExampleStreamReader_ReadSeriesWithIndexedInfo shows us to use an `Iterator`
// to read a stream footer and then using the index-info structs to retrieve
// specific series rather than iterating through them one at a time.
func ExampleStreamReader_ReadSeriesWithIndexedInfo() {
    b := rifs.NewSeekableBuffer()

    // Stage stream.

    sb := NewStreamBuilder(b)
    sb.sw.SetStructureLogging(true)

    series := AddTestSeries(sb)

    for i, seriesFooter := range series {
        fmt.Printf("Test series (%d): [%s]\n", i, seriesFooter.Uuid())
    }

    fmt.Printf("\n")

    _, err := sb.Finish()
    log.PanicIf(err)

    raw := b.Bytes()

    // Enumerate the stream.

    r := bytes.NewReader(raw)
    sr := NewStreamReader(r)

    it, err := NewIterator(sr)
    log.PanicIf(err)

    // Very cheap calls. Keep in mind that we will actually iterate through
    // these in reverse order, below.
    fmt.Printf("Number of series recorded in stream footer: (%d)\n", it.Count())

    // Read the first series, specifically.

    seriesNumber := 0

    sisi := it.SeriesInfo(seriesNumber)
    fmt.Printf("Indexed series (%d): %s\n", seriesNumber, sisi.Uuid())

    seriesData := new(bytes.Buffer)

    seriesFooter, _, checksumOk, err := sr.ReadSeriesWithIndexedInfo(sisi, seriesData)
    log.PanicIf(err)

    if checksumOk != true {
        log.Panicf("first series checksum does not match")
    }

    fmt.Printf("Series (%d): %s\n", seriesNumber, seriesFooter.Uuid())

    // This is the original time-series' blob. It's the caller's responsibility
    // to encode it and decode it.
    fmt.Printf("Series (%d) data: %s\n", seriesNumber, string(seriesData.Bytes()))

    fmt.Printf("\n")

    // Read the second series, specifically.

    seriesNumber = 1

    sisi = it.SeriesInfo(seriesNumber)
    fmt.Printf("Indexed series (%d): %s\n", seriesNumber, sisi.Uuid())

    seriesData = new(bytes.Buffer)

    seriesFooter, _, checksumOk, err = sr.ReadSeriesWithIndexedInfo(sisi, seriesData)
    log.PanicIf(err)

    if checksumOk != true {
        log.Panicf("second series checksum does not match")
    }

    fmt.Printf("Series (%d): %s\n", seriesNumber, seriesFooter.Uuid())

    // This is the original time-series' blob. It's the caller's responsibility
    // to encode it and decode it.
    fmt.Printf("Series (%d) data: %s\n", seriesNumber, string(seriesData.Bytes()))

    fmt.Printf("\n")

    // Output:
    // Test series (0): [d095abf5-126e-48a7-8974-885de92bd964]
    // Test series (1): [8a4ba0c4-0a0d-442f-8256-1d61adb16abc]
    //
    // Number of series recorded in stream footer: (2)
    // Indexed series (0): d095abf5-126e-48a7-8974-885de92bd964
    // Series (0): d095abf5-126e-48a7-8974-885de92bd964
    // Series (0) data: some time series data
    //
    // Indexed series (1): 8a4ba0c4-0a0d-442f-8256-1d61adb16abc
    // Series (1): 8a4ba0c4-0a0d-442f-8256-1d61adb16abc
    // Series (1) data: X some time series data 2 X
}
