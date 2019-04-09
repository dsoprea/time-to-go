package timetogo

import (
    "io"

    "github.com/dsoprea/go-logging"
)

type StreamBuilder struct {
    w      io.Writer
    sw     *StreamWriter
    series []SeriesFooter

    nextOffset int64
    offsets    []int64
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

// AddSeries adds a single series and associated metadata to the stream.
func (sb *StreamBuilder) AddSeries(encoded []byte, sf SeriesFooter) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    dataSize, err := sb.w.Write(encoded)
    log.PanicIf(err)

    footerSize, err := sb.sw.writeSeriesFooter1(sf)
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
