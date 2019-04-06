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
func (sr *StreamReader) readFooter() (sf interface{}, err error) {
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

    var footerVersion uint16
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
        sf := NewStreamFooter1FromEncoded(sfEncoded)

        return sf, nil
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
