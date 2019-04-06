package timetogo

import (
    "io"
    "os"

    "encoding/binary"

    "github.com/dsoprea/go-logging"
    "github.com/google/flatbuffers/go"

    "github.com/dsoprea/time-to-go/protocol/ttgstream"
)

var (
    streamLogger = log.NewLogger("timetogo.stream")
)

type SeriesFooterVersion uint16

// SeriesFooterVersion enum
const (
    SeriesFooterVersion1 SeriesFooterVersion = 1
)

// SeriesMetadata describes data derived from a stream footer.
type SeriesMetadata interface {
    // HeadRecordEpoch is the timestamp of the first record
    HeadRecordEpoch() uint64

    // TailRecordEpoch is the timestamp of the last record
    TailRecordEpoch() uint64

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

type StreamReader struct {
    rs io.ReadSeeker
}

func NewStreamReader(rs io.ReadSeeker) *StreamReader {
    return &StreamReader{
        rs: rs,
    }
}

type FooterType byte

const (
    FtSeriesFooter FooterType = 1
)

// readFooter reads a non-series block of data (e.g. series footer) backwards
// from the current position.
func (sr *StreamReader) readFooter() (footerVersion uint16, footerType FooterType, footerBytes []byte, err error) {
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

    footerTypeBytes := make([]byte, 1)
    _, err = io.ReadFull(sr.rs, footerTypeBytes)
    log.PanicIf(err)

    footerType = FooterType(footerTypeBytes[0])

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
func (sr *StreamReader) readSeriesFooter() (sm SeriesMetadata, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    footerVersion, footerType, footerBytes, err := sr.readFooter()
    log.PanicIf(err)

    if footerType != FtSeriesFooter {
        log.Panicf("next footer (reverse iteration) is not a series-footer")
    }

    switch footerVersion {
    case 1:
        sfEncoded := ttgstream.GetRootAsSeriesFooter1(footerBytes, 0)
        sm := NewSeriesFooter1FromEncoded(sfEncoded)

        return sm, nil
    }

    log.Panicf("footer version not valid (%d)", footerVersion)
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
