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

type StreamFooterVersion uint16

// StreamFooterVersion enum
const (
    StreamFooterVersion1 StreamFooterVersion = 1
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
    Version() StreamFooterVersion
}

type StreamReader struct {
    rs io.ReadSeeker
}

func NewStreamReader(rs io.ReadSeeker) *StreamReader {
    return &StreamReader{
        rs: rs,
    }
}

// readFooter will read the footer for the current series. When this returns,
// the current position will be the last byte of the time-series that precedes
// the footer. The last byte will always be a NUL.
func (sr *StreamReader) readFooter() (sm SeriesMetadata, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // We should always be sitting on a NUL.

    boundaryMarker := make([]byte, 1)

    _, err = sr.rs.Read(boundaryMarker)
    log.PanicIf(err)

    if boundaryMarker[0] != 0 {
        log.Panicf("not on a series boundary marker")
    }

    // Read the shadow footer.

    var twoBytes [2]uint16
    shadowFooterSize := int64(binary.Size(twoBytes))

    // We're expecting to start on the last byte of any of the shadow-footers
    // in the stream, which we've already read past, above.
    shadowPosition, err := sr.rs.Seek(-shadowFooterSize-1, os.SEEK_END)
    log.PanicIf(err)

    var footerVersion StreamFooterVersion
    err = binary.Read(sr.rs, binary.LittleEndian, &footerVersion)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "SHADOW: FOOTER-VERSION=(%d)", footerVersion)

    var footerLength uint16
    err = binary.Read(sr.rs, binary.LittleEndian, &footerLength)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "SHADOW: FOOTER-LENGTH=(%d)", footerLength)

    // Read the encoded footer.

    footerPosition, err := sr.rs.Seek(shadowPosition-int64(footerLength), os.SEEK_SET)
    log.PanicIf(err)

    footerBytes := make([]byte, footerLength)

    _, err = io.ReadFull(sr.rs, footerBytes)
    log.PanicIf(err)

    streamLogger.Debugf(nil, "Reading version (%d) footer of length (%d) at position (%d).", footerVersion, footerLength, footerPosition)

    switch footerVersion {
    case 1:
        sfEncoded := ttgstream.GetRootAsStreamFooter1(footerBytes, 0)
        sm := NewStreamFooter1FromEncoded(sfEncoded)

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
