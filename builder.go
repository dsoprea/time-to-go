package timetogo

import (
    "io"

    "hash/fnv"

    "github.com/dsoprea/go-logging"
)

const (
    // The size of the buffer to use for the copy of the time-series data into
    // the output stream.
    SeriesDataCopyBufferSize = 1024 * 1024
)

type StreamBuilder struct {
    w      io.Writer
    sw     *StreamWriter
    series []SeriesFooter

    nextOffset int64
    offsets    []int64

    copyBuffer []byte
}

func NewStreamBuilder(w io.Writer) *StreamBuilder {
    sw := NewStreamWriter(w)
    series := make([]SeriesFooter, 0)
    offsets := make([]int64, 0)

    return &StreamBuilder{
        w:       w,
        sw:      sw,
        series:  series,
        offsets: offsets,
    }
}

// AddSeries adds a single series and associated metadata to the stream. The
// actual series data is provided to us by the caller in serialized (encoded)
// form from whatever their original format was.
func (sb *StreamBuilder) AddSeries(encodedSeriesDataReader io.Reader, sf SeriesFooter) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    if sb.copyBuffer == nil {
        sb.copyBuffer = make([]byte, SeriesDataCopyBufferSize)
    }

    fnv1a := fnv.New32a()

    teeWriter := io.MultiWriter(sb.w, fnv1a)

    copiedCount, err := io.CopyBuffer(teeWriter, encodedSeriesDataReader, sb.copyBuffer)
    log.PanicIf(err)

    fnvChecksum := fnv1a.Sum32()

    // Make sure we copied as much as we expected to.
    expectedCount := sf.BytesLength()
    if uint64(copiedCount) != expectedCount {
        log.Panicf("series data size (%d) does not equal size (%d) in series footer", copiedCount, expectedCount)
    }

    footerSize, err := sb.sw.writeSeriesFooter1(sf, fnvChecksum)

    log.PanicIf(err)

    sb.nextOffset += int64(dataSize) + int64(footerSize)
    sb.offsets = append(sb.offsets, sb.nextOffset-1)

    sb.series = append(sb.series, sf)

    return nil
}

// Finish will finalize/complete the stream.
func (sb *StreamBuilder) Finish() (totalSize uint64, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // TODO(dustin): !! Update stream-footer to have create-time and last-update time.

    series := make([]StreamIndexedSequenceInfo, len(sb.series))
    for i, seriesFooter := range sb.series {
        sisi := NewStreamIndexedSequenceInfo1(
            seriesFooter.Uuid(),
            seriesFooter.HeadRecordTime(),
            seriesFooter.TailRecordTime(),
            seriesFooter.OriginalFilename(),
            sb.offsets[i])

        series[i] = sisi
    }

    footerSize, err := sb.sw.writeStreamFooter(series)
    log.PanicIf(err)

    // For completeness, step the offset.
    sb.nextOffset += int64(footerSize)

    return uint64(sb.nextOffset), nil
}
