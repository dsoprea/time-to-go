package timetogo

import (
	"fmt"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/google/flatbuffers/go"

	"github.com/dsoprea/time-to-go/protocol/ttgstream"
)

var (
	streamLogger1 = log.NewLogger("timetogo.stream_protocol_1")
)

// StreamIndexedSequenceInfo1 briefly describes all series.
type StreamIndexedSequenceInfo1 struct {
	// uuid uniquely identifies the series
	uuid string

	// headRecordTime is the timestamp of the first record
	headRecordTime time.Time

	// tailRecordTime is the timestamp of the last record
	tailRecordTime time.Time

	// originalFilename is the filename of the source-data
	originalFilename string

	// absolutePosition is the absolute position of the boundary marker (NUL)
	absolutePosition int64
}

// NewStreamIndexedSequenceInfo1 returns a sequence-info structure.
func NewStreamIndexedSequenceInfo1(uuid string, headRecordTime, tailRecordTime time.Time, originalFilename string, absolutePosition int64) *StreamIndexedSequenceInfo1 {
	return &StreamIndexedSequenceInfo1{
		uuid:             uuid,
		headRecordTime:   headRecordTime.UTC(),
		tailRecordTime:   tailRecordTime.UTC(),
		originalFilename: originalFilename,
		absolutePosition: absolutePosition,
	}
}

// NewStreamIndexedSequenceInfo1WithSeriesFooter returns a summary
// `StreamIndexedSequenceInfo1` struct representing the given
// `SeriesFooter`-compatible struct.
func NewStreamIndexedSequenceInfo1WithSeriesFooter(seriesFooter SeriesFooter, absolutePosition int64) *StreamIndexedSequenceInfo1 {
	return &StreamIndexedSequenceInfo1{
		uuid:             seriesFooter.Uuid(),
		headRecordTime:   seriesFooter.HeadRecordTime().UTC(),
		tailRecordTime:   seriesFooter.TailRecordTime().UTC(),
		originalFilename: seriesFooter.OriginalFilename(),
		absolutePosition: absolutePosition,
	}
}

// Uuid is the timestamp of the first record
func (sisi StreamIndexedSequenceInfo1) Uuid() string {
	return sisi.uuid
}

// HeadRecordTime is the timestamp of the first record
func (sisi StreamIndexedSequenceInfo1) HeadRecordTime() time.Time {
	return sisi.headRecordTime
}

// TailRecordTime is the timestamp of the last record
func (sisi StreamIndexedSequenceInfo1) TailRecordTime() time.Time {
	return sisi.tailRecordTime
}

// OriginalFilename is the filename of the source-data
func (sisi StreamIndexedSequenceInfo1) OriginalFilename() string {
	return sisi.originalFilename
}

// AbsolutePosition is the absolute position of the boundary marker (NUL)
func (sisi StreamIndexedSequenceInfo1) AbsolutePosition() int64 {
	return sisi.absolutePosition
}

func (sisi StreamIndexedSequenceInfo1) String() string {
	return fmt.Sprintf("StreamIndexedSequenceInfo1<UUID=[%s] HEAD=[%s] TAIL=[%s] FILENAME=[%s] POSITION=(%d)", sisi.uuid, sisi.headRecordTime, sisi.tailRecordTime, sisi.originalFilename, sisi.absolutePosition)
}

// writeStreamFooter writes a block of data that describes the entire stream.
func (sw *StreamWriter) writeStreamFooter(streamFooter StreamFooter) (size int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	sw.b.Reset()

	// Allocate series items.

	sequences := streamFooter.Series()

	sisiOffsets := make([]flatbuffers.UOffsetT, len(sequences))
	for i, sisi := range sequences {
		uuidPosition := sw.b.CreateString(sisi.Uuid())
		filenamePosition := sw.b.CreateString(sisi.OriginalFilename())

		ttgstream.StreamIndexedSequenceInfoStart(sw.b)
		ttgstream.StreamIndexedSequenceInfoAddUuid(sw.b, uuidPosition)
		ttgstream.StreamIndexedSequenceInfoAddHeadRecordEpoch(sw.b, uint64(sisi.HeadRecordTime().Unix()))
		ttgstream.StreamIndexedSequenceInfoAddTailRecordEpoch(sw.b, uint64(sisi.TailRecordTime().Unix()))
		ttgstream.StreamIndexedSequenceInfoAddOriginalFilename(sw.b, filenamePosition)
		ttgstream.StreamIndexedSequenceInfoAddAbsolutePosition(sw.b, sisi.AbsolutePosition())

		sisiOffset := ttgstream.StreamIndexedSequenceInfoEnd(sw.b)
		sisiOffsets[i] = sisiOffset
	}

	// Allocate vector.

	seriesCount := len(sequences)
	ttgstream.StreamFooter1StartSeriesVector(sw.b, seriesCount)

	for i := len(sisiOffsets) - 1; i >= 0; i-- {
		sisiOffset := sisiOffsets[i]
		sw.b.PrependUOffsetT(sisiOffset)
	}

	seriesVectorOffset := sw.b.EndVector(seriesCount)

	// Build footer.

	ttgstream.StreamFooter1Start(sw.b)

	ttgstream.StreamFooter1AddSeries(sw.b, seriesVectorOffset)

	sfPosition := ttgstream.StreamFooter1End(sw.b)

	sw.b.Finish(sfPosition)

	data := sw.b.FinishedBytes()
	streamLogger1.Debugf(nil, "Writing (%d) bytes for stream footer.", len(data))

	err = sw.pushStreamMilestone(MtStreamFooterHeadByte, fmt.Sprintf("Stream: %s", streamFooter))
	log.PanicIf(err)

	n, err := sw.w.Write(data)
	log.PanicIf(err)

	sw.bumpPosition(int64(n))

	footerVersion := uint16(1)
	shadowSize, err := sw.writeShadowFooter(footerVersion, FtStreamFooter, uint16(len(data)))
	log.PanicIf(err)

	size = len(data) + shadowSize
	return size, nil
}

func (sw *StreamWriter) writeStreamFooterWithSeriesFooters(series []SeriesFooter, offsets []int64) (footerSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	indexedSeries := make([]StreamIndexedSequenceInfo, len(series))
	for i, seriesFooter := range series {
		sisi :=
			NewStreamIndexedSequenceInfo1WithSeriesFooter(
				seriesFooter,
				offsets[i])

		indexedSeries[i] = sisi
	}

	streamFooter := NewStreamFooter1FromStreamIndexedSequenceInfoSlice(indexedSeries)

	footerSize, err = sw.writeStreamFooter(streamFooter)
	log.PanicIf(err)

	return footerSize, nil
}

// StreamFooter1 represents the stream footer (version 1) that's encoded in the
// stream.
type StreamFooter1 struct {
	series []StreamIndexedSequenceInfo
}

func (sf *StreamFooter1) String() string {
	return fmt.Sprintf("StreamFooter1<COUNT=(%d)>", len(sf.Series()))
}

// Series returns a list of all of the summary series information.
func (sf *StreamFooter1) Series() []StreamIndexedSequenceInfo {
	return sf.series
}

// NewStreamFooter1FromStreamIndexedSequenceInfoSlice returns a new
// `StreamFooter`-compatible struct.
func NewStreamFooter1FromStreamIndexedSequenceInfoSlice(series []StreamIndexedSequenceInfo) StreamFooter {
	sf := &StreamFooter1{
		series: series,
	}

	return sf
}

// NewStreamFooter1FromEncoded decodes the given bytes and returns a
// `StreamFooter`-compatible struct.
func NewStreamFooter1FromEncoded(footerBytes []byte) (sf StreamFooter, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	sfEncoded := ttgstream.GetRootAsStreamFooter1(footerBytes, 0)

	seriesCount := sfEncoded.SeriesLength()
	series := make([]StreamIndexedSequenceInfo, seriesCount)
	for i := 0; i < seriesCount; i++ {
		sisiEncoded := ttgstream.StreamIndexedSequenceInfo{}
		found := sfEncoded.Series(&sisiEncoded, i)
		if found == false {
			log.Panicf("could not find series (%d) info in stream info", i)
		}

		headRecordTime := time.Unix(int64(sisiEncoded.HeadRecordEpoch()), 0).In(time.UTC)
		tailRecordTime := time.Unix(int64(sisiEncoded.TailRecordEpoch()), 0).In(time.UTC)

		sisi := &StreamIndexedSequenceInfo1{
			headRecordTime:   headRecordTime,
			tailRecordTime:   tailRecordTime,
			originalFilename: string(sisiEncoded.OriginalFilename()),
			absolutePosition: sisiEncoded.AbsolutePosition(),
		}

		series[i] = sisi
	}

	sf = NewStreamFooter1FromStreamIndexedSequenceInfoSlice(series)
	return sf, nil
}
