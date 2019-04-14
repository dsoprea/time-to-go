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
	// TODO(dustin): !! Start returning an error value.

	// We need this to make sure that our writes are always performd in the
	// correct place. This enables us to copy existing data from later positions
	// in the file.
	// TODO(dustin): !! Move the writing to StreamWriter so we can both keep track of the file position, so we can keep track of the structure.

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

func (sb *StreamBuilder) SetStructureLogging(flag bool) {
	sb.sw.SetStructureLogging(flag)
}

func (sb *StreamBuilder) Structure() *StreamStructure {
	return sb.sw.Structure()
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

	err = sb.sw.pushSeriesMilestone(-1, MtSeriesDataHeadByte, sf.Uuid(), "")
	log.PanicIf(err)

	sb.sw.bumpPosition(int64(copiedCount))

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

// NextOffset returns the position that the head bytes
func (sb *StreamBuilder) NextOffset() int64 {
	return sb.nextOffset
}

// AddSeriesNoWrite logs a single series and associated metadata but doesn't
// actually write. It will be written through other means.
func (sb *StreamBuilder) AddSeriesNoWrite(footerDataPosition int64, totalSeriesSize int, sf SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	err = sb.sw.pushSeriesMilestone(footerDataPosition, MtSeriesDataHeadByte, sf.Uuid(), "")
	log.PanicIf(err)

	footerPosition := footerDataPosition + int64(sf.BytesLength())

	err = sb.sw.pushSeriesMilestone(footerPosition, MtSeriesFooterHeadByte, sf.Uuid(), "(Retained during update)")
	log.PanicIf(err)

	sb.sw.bumpPosition(int64(totalSeriesSize))

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
