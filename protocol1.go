package timetogo

import (
    "encoding/binary"

    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/time-to-go/protocol/ttgstream"
)

type StreamFooter1 struct {
    headRecordEpoch   uint64   // The timestamp of the first record
    tailRecordEpoch   uint64   // The timestamp of the last record
    bytesLength       uint64   // The number of bytes occupied on-disk
    recordCount       uint64   // The number of records in the list
    originalFilename  string   // The filename of the source-data
    sourceSha1        [20]byte // SHA1 of the raw source-data; can be used to determine if the source-data has changed
    dataFnv1aChecksum uint32   // FNV-1a checksum of the time-series data on-disk
}

func NewStreamFooter1(headRecordEpoch, tailRecordEpoch, bytesLength, recordCount uint64, originalFilename string, sourceSha1 [20]byte, dataFnv1aChecksum uint32) *StreamFooter1 {
    return &StreamFooter1{
        headRecordEpoch:   headRecordEpoch,
        tailRecordEpoch:   tailRecordEpoch,
        bytesLength:       bytesLength,
        recordCount:       recordCount,
        originalFilename:  originalFilename,
        sourceSha1:        sourceSha1,
        dataFnv1aChecksum: dataFnv1aChecksum,
    }
}

func NewStreamFooter1FromEncoded(sfEncoded *ttgstream.StreamFooter1) (sf *StreamFooter1) {
    sha1ByteCount := sfEncoded.SourceSha1Length()
    if sha1ByteCount != 20 {
        log.PanicIf("SHA1 is not the right size")
    }

    sf = &StreamFooter1{
        headRecordEpoch:   sfEncoded.HeadRecordEpoch(),
        tailRecordEpoch:   sfEncoded.TailRecordEpoch(),
        bytesLength:       sfEncoded.BytesLength(),
        recordCount:       sfEncoded.RecordCount(),
        originalFilename:  string(sfEncoded.OriginalFilename()),
        dataFnv1aChecksum: sfEncoded.DataFnv1aChecksum(),
    }

    for i := 0; i < 20; i++ {
        b := sfEncoded.SourceSha1(i)
        sf.sourceSha1[i] = byte(b)
    }

    return sf
}

// writeFooter1 will write the footer for a series. When this returns, we'll be
// in the position following the final NUL byte.
func (sw *StreamWriter) writeFooter1(sf *StreamFooter1) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    sw.b.Reset()

    filenamePosition := sw.b.CreateString(sf.originalFilename)
    sha1Position := sw.b.CreateByteString(sf.sourceSha1[:])

    ttgstream.StreamFooter1Start(sw.b)
    ttgstream.StreamFooter1AddHeadRecordEpoch(sw.b, sf.headRecordEpoch)
    ttgstream.StreamFooter1AddTailRecordEpoch(sw.b, sf.tailRecordEpoch)
    ttgstream.StreamFooter1AddBytesLength(sw.b, sf.bytesLength)
    ttgstream.StreamFooter1AddRecordCount(sw.b, sf.recordCount)
    ttgstream.StreamFooter1AddOriginalFilename(sw.b, filenamePosition)
    ttgstream.StreamFooter1AddSourceSha1(sw.b, sha1Position)
    ttgstream.StreamFooter1AddDataFnv1aChecksum(sw.b, sf.dataFnv1aChecksum)
    sfPosition := ttgstream.StreamFooter1End(sw.b)

    sw.b.Finish(sfPosition)

    data := sw.b.FinishedBytes()

    _, err = sw.w.Write(data)
    log.PanicIf(err)

    footerVersion := uint16(1)
    err = binary.Write(sw.w, binary.LittleEndian, footerVersion)
    log.PanicIf(err)

    footerLength := uint16(len(data))
    err = binary.Write(sw.w, binary.LittleEndian, footerLength)
    log.PanicIf(err)

    _, err = sw.w.Write([]byte{0})
    log.PanicIf(err)

    return nil
}
