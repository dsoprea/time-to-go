package timetogo

import (
    "io"
    "time"

    "encoding/binary"
    "os"

    "github.com/dsoprea/go-logging"
    "github.com/google/flatbuffers/go"
)

var (
    streamLogger = log.NewLogger("timetogo.stream")
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
    // HeadRecordTime is the timestamp of the first record
    HeadRecordTime() time.Time

    // TailRecordTime is the timestamp of the last record
    TailRecordTime() time.Time

    // OriginalFilename is the filename of the source-data
    OriginalFilename() string

    // AbsolutePosition is the absolute position of the boundary marker (NUL)
    AbsolutePosition() uint64
}

type StreamFooter interface {
    Series() []StreamIndexedSequenceInfo
}

type StreamReader struct {
    rs io.ReadSeeker
}

func NewStreamReader(rs io.ReadSeeker) *StreamReader {
    return &StreamReader{
        rs: rs,
    }
}

// readFooter reads a non-series block of data (e.g. series footer) backwards
// from the current position.
func (sr *StreamReader) readShadowFooter() (footerVersion uint16, footerType FooterType, footerBytes []byte, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // TODO(dustin): !! Add test.

    // We should always be sitting on a NUL.

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
    shadowPosition, err := sr.rs.Seek(-int64(shadowFooterSize)-1, os.SEEK_END)
    log.PanicIf(err)

    err = binary.Read(sr.rs, binary.LittleEndian, &footerVersion)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "SHADOW: FOOTER-VERSION=(%d)", footerVersion)

    err = binary.Read(sr.rs, binary.LittleEndian, &footerType)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "SHADOW: FOOTER-TYPE=(%d)", footerType)

    var footerLength uint16
    err = binary.Read(sr.rs, binary.LittleEndian, &footerLength)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "SHADOW: FOOTER-LENGTH=(%d)", footerLength)

    // Read the encoded footer.

    footerPosition, err := sr.rs.Seek(shadowPosition-int64(footerLength), os.SEEK_SET)
    log.PanicIf(err)

    footerBytes = make([]byte, footerLength)

    _, err = io.ReadFull(sr.rs, footerBytes)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "Reading version (%d) footer of length (%d) at position (%d).", footerVersion, footerLength, footerPosition)

    return footerVersion, footerType, footerBytes, nil
}

// readSeriesFooter will read the footer for the current series. When this
// returns, the current position will be the last byte of the time-series that
// precedes the footer. The last byte will always be a NUL.
func (sr *StreamReader) readSeriesFooter() (sf SeriesFooter, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    footerVersion, footerType, footerBytes, err := sr.readShadowFooter()
    log.PanicIf(err)

    if footerType != FtSeriesFooter {
        log.Panicf("next footer (reverse iteration) is not a series-footer")
    }

    switch footerVersion {
    case 1:
        sm := NewSeriesFooter1FromEncoded(footerBytes)

        return sm, nil
    }

    log.Panicf("series footer version not valid (%d)", footerVersion)
    panic(nil)
}

// readStreamFooter parses data located at the very end of the stream that
// describes the contents of the stream.
func (sr *StreamReader) readStreamFooter() (sf StreamFooter, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    footerVersion, footerType, footerBytes, err := sr.readShadowFooter()
    log.PanicIf(err)

    if footerType != FtStreamFooter {
        log.Panicf("next footer (reverse iteration) is not a stream-footer")
    }

    switch footerVersion {
    case 1:
        sf, err := NewStreamFooter1FromEncoded(footerBytes)
        log.PanicIf(err)

        return sf, nil
    }

    log.Panicf("stream footer version not valid (%d)", footerVersion)
    panic(nil)
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
func (sw *StreamWriter) writeShadowFooter(footerVersion uint16, footerType FooterType, footerLength uint16) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    err = binary.Write(sw.w, binary.LittleEndian, footerVersion)
    log.PanicIf(err)

    err = binary.Write(sw.w, binary.LittleEndian, footerType)
    log.PanicIf(err)

    err = binary.Write(sw.w, binary.LittleEndian, footerLength)
    log.PanicIf(err)

    _, err = sw.w.Write([]byte{0})
    log.PanicIf(err)

    return nil
}
