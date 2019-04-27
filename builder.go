package timetogo

import (
	"io"
	"os"
	"reflect"

	"hash/fnv"

	"github.com/dsoprea/go-logging"
)

// StreamBuilder is the high-level interface that owns the stream-building
// process and wraps `StreamWriter`.
type StreamBuilder struct {
	ws     io.WriteSeeker
	sw     *StreamWriter
	series []SeriesFooter

	nextOffset int64
	offsets    []int64

	copyBuffer []byte
}

// NewStreamBuilder returns a new `StreamBuilder`.
func NewStreamBuilder(ws io.WriteSeeker) *StreamBuilder {
	// We need this to make sure that our writes are always performd in the
	// correct place. This enables us to copy existing data from later positions
	// in the file.

	sw := NewStreamWriter(ws)
	series := make([]SeriesFooter, 0)
	offsets := make([]int64, 0)

	return &StreamBuilder{
		ws:      ws,
		sw:      sw,
		series:  series,
		offsets: offsets,
	}
}

// SetStructureLogging enables/disables structure tracking.
func (sb *StreamBuilder) SetStructureLogging(flag bool) {
	sb.sw.SetStructureLogging(flag)
}

// Structure returns the `StreamStructure` struct (if enabled).
func (sb *StreamBuilder) Structure() *StreamStructure {
	return sb.sw.Structure()
}

// StreamWriter returns the underlying `StreamWriter` struct.
func (sb *StreamBuilder) StreamWriter() *StreamWriter {
	return sb.sw
}

// AddSeries adds a single series and associated metadata to the stream. The
// actual series data is provided to us by the caller in serialized (encoded)
// form from whatever their original format was.
func (sb *StreamBuilder) AddSeries(seriesDataWriter interface{}, sf SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// NOTE(dustin): Note that we don't perform the same current-position check
	// that we do at the bottom and at the top and bottom of the other function
	// because we're not currently guaranteed to be at that position. The
	// bounceback-writer will put us where we need to be.

	if sb.copyBuffer == nil {
		sb.copyBuffer = make([]byte, SeriesDataCopyBufferSize)
	}

	err = sb.sw.pushSeriesMilestone(-1, MtSeriesDataHeadByte, sf.Uuid(), "")
	log.PanicIf(err)

	fnv1a := fnv.New32a()

	teeWriter := io.MultiWriter(sb.sw, fnv1a)

	var copiedCount uint64
	switch t := seriesDataWriter.(type) {
	case SeriesDataDatasourceWriter:
		var err error
		n, err := t.WriteData(teeWriter, sf)
		log.PanicIf(err)

		copiedCount = uint64(n)

	case io.Reader:
		n, err := io.Copy(teeWriter, t)
		log.PanicIf(err)

		copiedCount = uint64(n)

	default:
		log.Panicf("series-data writer is not the right type: %s", reflect.TypeOf(seriesDataWriter))
	}

	fnvChecksum := fnv1a.Sum32()

	sf.SetBytesLength(uint64(copiedCount))

	footerSize, err := sb.sw.writeSeriesFooter1(sf, fnvChecksum)
	log.PanicIf(err)

	totalSeriesSize := int(copiedCount) + footerSize
	sb.nextOffset += int64(totalSeriesSize)

	// NOTE(dustin): Keep this and the check below for now.
	position, err := sb.ws.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	if position != sb.nextOffset {
		log.Panicf("final position is not equal to next-offset (write): (%d) != (%d)", position, sb.nextOffset)
	}

	sb.offsets = append(sb.offsets, sb.nextOffset-1)

	sb.series = append(sb.series, sf)

	return nil
}

// NextOffset returns the position that the head bytes
func (sb *StreamBuilder) NextOffset() int64 {
	return sb.nextOffset
}

// AddSeriesNoWrite logs a single series and associated metadata but doesn't
// actually write. It will be written (or potentially retained) through other
// means.
func (sb *StreamBuilder) AddSeriesNoWrite(footerDataPosition int64, totalSeriesSize int, sf SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// NOTE(dustin): Keep this and the check below for now.
	initialPosition, err := sb.ws.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	if initialPosition != sb.nextOffset {
		log.Panicf("initial position is not correct: (%d) != (%d)", initialPosition, sb.nextOffset)
	}

	err = sb.sw.pushSeriesMilestone(footerDataPosition, MtSeriesDataHeadByte, sf.Uuid(), "")
	log.PanicIf(err)

	footerPosition := footerDataPosition + int64(sf.BytesLength())

	err = sb.sw.pushSeriesMilestone(footerPosition, MtSeriesFooterHeadByte, sf.Uuid(), "(Retained during update)")
	log.PanicIf(err)

	// Decrement by the size of the shadow footer, which includes the boundary
	// marker, so we can add those as separate entries.
	sb.sw.bumpPosition(int64(totalSeriesSize - ShadowFooterSize))

	err = sb.sw.pushSeriesMilestone(-1, MtShadowFooterHeadByte, sf.Uuid(), "(Retained during update)")
	log.PanicIf(err)

	sb.sw.bumpPosition(ShadowFooterSize - 1)

	err = sb.sw.pushSeriesMilestone(-1, MtBoundaryMarker, sf.Uuid(), "(Retained during update)")
	log.PanicIf(err)

	sb.sw.bumpPosition(1)

	sb.nextOffset += int64(totalSeriesSize)

	// Bump the file position.

	finalPosition, err := sb.ws.Seek(int64(totalSeriesSize), os.SEEK_CUR)
	log.PanicIf(err)

	if finalPosition != sb.nextOffset {
		log.Panicf("final position is not expected (no-write): (%d) != (%d)", finalPosition, sb.nextOffset)
	}

	sb.offsets = append(sb.offsets, sb.nextOffset-1)
	sb.series = append(sb.series, sf)

	// NOTE(dustin): Keep this and the check below for now.
	position, err := sb.ws.Seek(0, os.SEEK_CUR)
	log.PanicIf(err)

	if position != sb.nextOffset {
		log.Panicf("final position is not equal to next-offset: (%d) != (%d)", position, sb.nextOffset)
	}

	return nil
}

// Finish will finalize/complete the stream.
func (sb *StreamBuilder) Finish() (totalSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	footerSize, err := sb.sw.writeStreamFooterWithSeriesFooters(sb.series, sb.offsets)
	log.PanicIf(err)

	// For completeness, step the offset.
	sb.nextOffset += int64(footerSize)

	return int(sb.nextOffset), nil
}
