package timetogo

import (
	"fmt"
	"io"
	"os"
	"time"

	"encoding/binary"
	"hash/fnv"

	"github.com/dsoprea/go-logging"
	"github.com/google/flatbuffers/go"
)

var (
	streamLogger = log.NewLogger("timetogo.stream")
)

const (
	// ShadowFooterSize is the size of the shadow footer:
	//
	//   version + type + length + boundary marker
	//
	ShadowFooterSize = 2 + 1 + 2 + 1
)

// SeriesFooterVersion enum
type SeriesFooterVersion uint16

const (
	SeriesFooterVersion1 SeriesFooterVersion = 1
)

// StreamFooterVersion enum
type StreamFooterVersion uint16

const (
	StreamFooterVersion1 StreamFooterVersion = 1
)

// FooterType is an enum that represents all footer types.
type FooterType byte

const (
	FtSeriesFooter FooterType = 1
	FtStreamFooter FooterType = 2
)

// SeriesFooter describes data derived from a stream footer.
type SeriesFooter interface {
	// Uuid is a unique string that uniquely identifies this series.
	Uuid() string

	// HeadRecordTime is the timestamp of the first record
	HeadRecordTime() time.Time

	// TailRecordTime is the timestamp of the last record
	TailRecordTime() time.Time

	// BytesLength() is the number of bytes occupied on-disk
	BytesLength() uint64

	// RecordCount is the number of records in the list
	RecordCount() uint64

	// OriginalFilename is the filename of the source-data
	OriginalFilename() string

	// SourceSha1 is the SHA1 of the raw source-data; can be used to determine
	// if the source-data has changed
	SourceSha1() []byte

	// DataFnv1aChecksum is the FNV-1a checksum of the time-series data on-disk
	DataFnv1aChecksum() uint32

	// Version returns the version of the footer.
	Version() SeriesFooterVersion
}

type StreamIndexedSequenceInfo interface {
	// Uuid is a unique string that uniquely identifies this series.
	Uuid() string

	// HeadRecordTime is the timestamp of the first record
	HeadRecordTime() time.Time

	// TailRecordTime is the timestamp of the last record
	TailRecordTime() time.Time

	// OriginalFilename is the filename of the source-data
	OriginalFilename() string

	// AbsolutePosition is the absolute position of the boundary marker (NUL)
	AbsolutePosition() int64
}

type StreamFooter interface {
	Series() []StreamIndexedSequenceInfo
}

type StreamReader struct {
	rs io.ReadSeeker
	ss *StreamStructure
}

func NewStreamReader(rs io.ReadSeeker) *StreamReader {
	return &StreamReader{
		rs: rs,
	}
}

func (sr *StreamReader) SetStructureLogging(flag bool) {
	if flag == true {
		sr.ss = NewStreamStructure()
	} else {
		sr.ss = nil
	}
}

func (sr *StreamReader) Structure() *StreamStructure {
	if sr.ss == nil {
		log.Panicf("not collecting structure info")
	}

	return sr.ss
}

// pushStreamMilestone records a milestone pertaining to the stream.
func (sr *StreamReader) pushStreamMilestone(position int64, milestoneType MilestoneType, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sr.ss != nil {
		if position == -1 {
			var err error
			position, err = sr.rs.Seek(0, os.SEEK_CUR)
			log.PanicIf(err)
		}

		sr.ss.Push(position, milestoneType, StStream, "", comment)
	}

	return nil
}

// pushSeriesMilestone records a milestone of a constituent series. The UUID is
// optional as it will not be known until partway through the process.
func (sr *StreamReader) pushSeriesMilestone(position int64, milestoneType MilestoneType, seriesUuid, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sr.ss != nil {
		if position == -1 {
			var err error
			position, err = sr.rs.Seek(0, os.SEEK_CUR)
			log.PanicIf(err)
		}

		sr.ss.Push(position, milestoneType, StSeries, seriesUuid, comment)
	}

	return nil
}

// pushStreamMilestone records a milestone pertaining to the stream.
func (sr *StreamReader) pushMiscMilestone(position int64, milestoneType MilestoneType, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sr.ss != nil {
		if position == -1 {
			var err error
			position, err = sr.rs.Seek(0, os.SEEK_CUR)
			log.PanicIf(err)
		}

		sr.ss.Push(position, milestoneType, StMisc, "", comment)
	}

	return nil
}

// readOneFooter reads backwards from the current position (which should be the
// NUL boundary marker). It will first read the shadow footer and then the raw
// bytes of the real footer preceding it.
func (sr *StreamReader) readOneFooter() (footerVersion uint16, footerType FooterType, footerBytes []byte, footerOffset int64, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add test.

	// We should always be sitting on a NUL.

	err = sr.pushMiscMilestone(-1, MtBoundaryMarker, "")
	log.PanicIf(err)

	boundaryMarker := make([]byte, 1)

	_, err = sr.rs.Read(boundaryMarker)
	log.PanicIf(err)

	if boundaryMarker[0] != 0 {
		log.Panicf("not on a series boundary marker")
	}

	// Read the shadow footer.

	// version + type + size
	shadowFooterSize := 2 + 1 + 2

	// We're expecting to start on the last byte of any of the shadow-footers
	// in the stream, which we've already read past, above.
	shadowPosition, err := sr.rs.Seek(-int64(shadowFooterSize)-1, os.SEEK_CUR)
	log.PanicIf(err)

	err = sr.pushMiscMilestone(shadowPosition, MtShadowFooterHeadByte, "")
	log.PanicIf(err)

	err = binary.Read(sr.rs, binary.LittleEndian, &footerVersion)
	log.PanicIf(err)

	err = binary.Read(sr.rs, binary.LittleEndian, &footerType)
	log.PanicIf(err)

	var footerLength uint16
	err = binary.Read(sr.rs, binary.LittleEndian, &footerLength)
	log.PanicIf(err)

	// Read the encoded footer.

	absoluteFooterOffset := shadowPosition - int64(footerLength)

	_, err = sr.rs.Seek(absoluteFooterOffset, os.SEEK_SET)
	log.PanicIf(err)

	streamLogger.Debugf(nil, "Footer: VERSION=(%d) TYPE=(%d) LENGTH=(%d) POSITION=(%d)", footerVersion, footerType, footerLength, absoluteFooterOffset)

	err = sr.pushMiscMilestone(absoluteFooterOffset, MtFooterHeadByte, "")
	log.PanicIf(err)

	footerBytes = make([]byte, footerLength)

	_, err = io.ReadFull(sr.rs, footerBytes)
	log.PanicIf(err)

	streamLogger.Debugf(nil, "Reading version (%d) footer of length (%d) at position (%d).", footerVersion, footerLength, absoluteFooterOffset)

	return footerVersion, footerType, footerBytes, absoluteFooterOffset, nil
}

// readSeriesFooter will read the footer for the current series. When this
// returns, the current position will be the last byte of the time-series that
// precedes the footer. The last byte will always be a NUL.
func (sr *StreamReader) readSeriesFooter() (sf SeriesFooter, dataOffset int64, nextBoundaryOffset int64, totalFooterSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	err = sr.pushSeriesMilestone(-1, MtBoundaryMarker, "", "")
	log.PanicIf(err)

	seriesFooterVersion, footerType, footerBytes, footerOffset, err := sr.readOneFooter()
	log.PanicIf(err)

	err = sr.pushSeriesMilestone(footerOffset, MtSeriesFooterHeadByte, "", "")
	log.PanicIf(err)

	if footerType != FtSeriesFooter {
		log.Panicf("next footer (reverse iteration) is not a series-footer: (%d)", footerType)
	}

	switch seriesFooterVersion {
	case 1:
		sf, err = NewSeriesFooter1FromEncoded(footerBytes)
		log.PanicIf(err)
	default:
		log.Panicf("series footer version not valid (%d)", seriesFooterVersion)
	}

	err = sr.pushSeriesMilestone(footerOffset, MtSeriesFooterDecoded, sf.Uuid(), "")
	log.PanicIf(err)

	dataOffset = footerOffset - int64(sf.BytesLength())
	nextBoundaryOffset = dataOffset - 1

	if nextBoundaryOffset >= 0 {
		_, err = sr.rs.Seek(nextBoundaryOffset, os.SEEK_SET)
		log.PanicIf(err)
	}

	totalFooterSize = len(footerBytes) + ShadowFooterSize
	return sf, dataOffset, nextBoundaryOffset, totalFooterSize, nil
}

// readStreamFooter parses data located at the very end of the stream that
// describes the contents of the stream.
func (sr *StreamReader) readStreamFooter() (sf StreamFooter, nextBoundaryOffset int64, totalFooterSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	err = sr.pushStreamMilestone(-1, MtBoundaryMarker, "")
	log.PanicIf(err)

	streamFooterVersion, footerType, footerBytes, footerOffset, err := sr.readOneFooter()
	log.PanicIf(err)

	err = sr.pushStreamMilestone(footerOffset, MtStreamFooterHeadByte, "")
	log.PanicIf(err)

	if footerType != FtStreamFooter {
		log.Panicf("next footer (reverse iteration) is not a stream-footer: (%d)", footerType)
	}

	switch streamFooterVersion {
	case 1:
		sf, err = NewStreamFooter1FromEncoded(footerBytes)
		log.PanicIf(err)

	default:
		log.Panicf("stream footer version not valid (%d)", streamFooterVersion)
		panic(nil)
	}

	err = sr.pushStreamMilestone(footerOffset, MtStreamFooterDecoded, fmt.Sprintf("Stream: %s", sf))
	log.PanicIf(err)

	nextBoundaryOffset = footerOffset - 1

	if nextBoundaryOffset >= 0 {
		_, err = sr.rs.Seek(nextBoundaryOffset, os.SEEK_SET)
		log.PanicIf(err)
	}

	totalFooterSize = len(footerBytes) + ShadowFooterSize
	return sf, nextBoundaryOffset, totalFooterSize, nil
}

func (sr *StreamReader) ReadSeriesInfoWithBoundaryPosition(position int64) (seriesFooter SeriesFooter, dataOffset int64, seriesSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add unit-test.

	_, err = sr.rs.Seek(position, os.SEEK_SET)
	log.PanicIf(err)

	seriesFooter, dataOffset, _, footerSize, err := sr.readSeriesFooter()
	log.PanicIf(err)

	err = sr.pushSeriesMilestone(dataOffset, MtSeriesDataHeadByte, seriesFooter.Uuid(), "")
	log.PanicIf(err)

	seriesSize = footerSize + int(seriesFooter.BytesLength())
	return seriesFooter, dataOffset, seriesSize, nil
}

func (sr *StreamReader) ReadSeriesInfoWithIndexedInfo(sisi StreamIndexedSequenceInfo) (seriesFooter SeriesFooter, dataOffset int64, seriesSize int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add unit-test.

	seriesFooter, dataOffset, seriesSize, err = sr.ReadSeriesInfoWithBoundaryPosition(sisi.AbsolutePosition())
	log.PanicIf(err)

	return seriesFooter, dataOffset, seriesSize, nil
}

func (sr *StreamReader) ReadSeriesWithIndexedInfo(sisi StreamIndexedSequenceInfo, dataWriter io.Writer) (seriesFooter SeriesFooter, seriesSize int, checksumOk bool, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add unit-test.

	seriesFooter, dataOffset, seriesSize, err := sr.ReadSeriesInfoWithIndexedInfo(sisi)
	log.PanicIf(err)

	// This is at the very front of all of the related data and metadata for
	// this series (time-series data, then footer, then shadow footer).
	_, err = sr.rs.Seek(dataOffset, os.SEEK_SET)
	log.PanicIf(err)

	// Calculate the checksum.

	fnv1a := fnv.New32a()

	var finalWriter io.Writer
	if dataWriter != nil {
		finalWriter = io.MultiWriter(dataWriter, fnv1a)
	} else {
		finalWriter = fnv1a
	}

	_, err = io.CopyN(finalWriter, sr.rs, int64(seriesFooter.BytesLength()))
	log.PanicIf(err)

	fnvChecksum := fnv1a.Sum32()

	checksumOk = fnvChecksum == seriesFooter.DataFnv1aChecksum()

	return seriesFooter, seriesSize, checksumOk, nil
}

// Reset will put us at the end of the file. This is required in order to
// iterate.
func (sr *StreamReader) Reset() (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// Put us on the trailing NUL byte.
	_, err = sr.rs.Seek(-1, os.SEEK_END)
	log.PanicIf(err)

	return nil
}

type StreamWriter struct {
	w io.Writer
	b *flatbuffers.Builder
}

func NewStreamWriter(w io.Writer) *StreamWriter {
	b := flatbuffers.NewBuilder(0)

	return &StreamWriter{
		w: w,
		b: b,
	}
}

// writeShadowFooter writes a statically-sized footer that follows and describes
// a dynamically-sized footer.
func (sw *StreamWriter) writeShadowFooter(footerVersion uint16, footerType FooterType, footerLength uint16) (size int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	err = binary.Write(sw.w, binary.LittleEndian, footerVersion)
	log.PanicIf(err)

	size += 2

	err = binary.Write(sw.w, binary.LittleEndian, footerType)
	log.PanicIf(err)

	size += 1

	err = binary.Write(sw.w, binary.LittleEndian, footerLength)
	log.PanicIf(err)

	size += 2

	_, err = sw.w.Write([]byte{0})
	log.PanicIf(err)

	size += 1

	streamLogger.Debugf(nil, "writeShadowFooter: Wrote (%d) bytes for shadow footer.", size)

	// Keep us honest.
	if size != ShadowFooterSize {
		log.Panicf("shadow footer is not the right size")
	}

	return size, nil
}
