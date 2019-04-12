package timetogo

import (
	"io"

	"hash/fnv"

	"github.com/dsoprea/go-logging"
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

	totalSeriesSize := int(copiedCount) + footerSize
	sb.nextOffset += int64(totalSeriesSize)
	sb.offsets = append(sb.offsets, sb.nextOffset-1)

	sb.series = append(sb.series, sf)

	return nil
}

// AddSeriesNoWrite logs a single series and associated metadata but doesn't
// actually write. It will be written through other means.
func (sb *StreamBuilder) AddSeriesNoWrite(totalSeriesSize int, sf SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	sb.nextOffset += int64(totalSeriesSize)
	sb.offsets = append(sb.offsets, sb.nextOffset-1)

	sb.series = append(sb.series, sf)

	return nil
}

// Finish will finalize/complete the stream.
func (sb *StreamBuilder) Finish() (totalSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Update stream-footer to have create-time and last-update time.

	footerSize, err := sb.sw.writeStreamFooterWithSeriesFooters(sb.series, sb.offsets)
	log.PanicIf(err)

	// For completeness, step the offset.
	sb.nextOffset += int64(footerSize)

	return int(sb.nextOffset), nil
}
