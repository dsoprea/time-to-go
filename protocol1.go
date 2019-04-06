package timetogo

import (
    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/time-to-go/protocol/ttgstream"
)

type SeriesFooter1 struct {
    headRecordEpoch   uint64 // The timestamp of the first record
    tailRecordEpoch   uint64 // The timestamp of the last record
    bytesLength       uint64 // The number of bytes occupied on-disk
    recordCount       uint64 // The number of records in the list
    originalFilename  string // The filename of the source-data
    sourceSha1        []byte // SHA1 of the raw source-data; can be used to determine if the source-data has changed
    dataFnv1aChecksum uint32 // FNV-1a checksum of the time-series data on-disk
}

func NewSeriesFooter1(headRecordEpoch, tailRecordEpoch, bytesLength, recordCount uint64, originalFilename string, sourceSha1 []byte, dataFnv1aChecksum uint32) *SeriesFooter1 {
    return &SeriesFooter1{
        headRecordEpoch:   headRecordEpoch,
        tailRecordEpoch:   tailRecordEpoch,
        bytesLength:       bytesLength,
        recordCount:       recordCount,
        originalFilename:  originalFilename,
        sourceSha1:        sourceSha1,
        dataFnv1aChecksum: dataFnv1aChecksum,
    }
}

func NewSeriesFooter1FromEncoded(sfEncoded *ttgstream.SeriesFooter1) (sf *SeriesFooter1) {
    sf = &SeriesFooter1{
        headRecordEpoch:   sfEncoded.HeadRecordEpoch(),
        tailRecordEpoch:   sfEncoded.TailRecordEpoch(),
        bytesLength:       sfEncoded.BytesLength(),
        recordCount:       sfEncoded.RecordCount(),
        originalFilename:  string(sfEncoded.OriginalFilename()),
        dataFnv1aChecksum: sfEncoded.DataFnv1aChecksum(),
        sourceSha1:        sfEncoded.SourceSha1(),
    }

    return sf
}

func (sf *SeriesFooter1) Version() SeriesFooterVersion {
    return SeriesFooterVersion1
}

func (sf *SeriesFooter1) HeadRecordEpoch() uint64 {
    return sf.headRecordEpoch
}

func (sf *SeriesFooter1) TailRecordEpoch() uint64 {
    return sf.tailRecordEpoch
}

func (sf *SeriesFooter1) BytesLength() uint64 {
    return sf.bytesLength
}

func (sf *SeriesFooter1) RecordCount() uint64 {
    return sf.recordCount
}

func (sf *SeriesFooter1) OriginalFilename() string {
    return sf.originalFilename
}

func (sf *SeriesFooter1) SourceSha1() []byte {
    return sf.sourceSha1
}

func (sf *SeriesFooter1) DataFnv1aChecksum() uint32 {
    return sf.dataFnv1aChecksum
}

// writeFooter takes a metadata-type struct and writes the latest version of
// the footer.
func (sw *StreamWriter) writeSeriesFooter(sm SeriesMetadata) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    err = sw.writeSeriesFooter1(sm)
    log.PanicIf(err)

    return nil
}

// writeFooter1 will write the footer for a series. When this returns, we'll be
// in the position following the final NUL byte.
func (sw *StreamWriter) writeSeriesFooter1(sm SeriesMetadata) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // TDO(dustin): !! We also need to finish off the stream with a metadata struct.

    sw.b.Reset()

    filenamePosition := sw.b.CreateString(sm.OriginalFilename())
    sha1Position := sw.b.CreateByteString(sm.SourceSha1())

    ttgstream.SeriesFooter1Start(sw.b)
    ttgstream.SeriesFooter1AddHeadRecordEpoch(sw.b, sm.HeadRecordEpoch())
    ttgstream.SeriesFooter1AddTailRecordEpoch(sw.b, sm.TailRecordEpoch())
    ttgstream.SeriesFooter1AddBytesLength(sw.b, sm.BytesLength())
    ttgstream.SeriesFooter1AddRecordCount(sw.b, sm.RecordCount())
    ttgstream.SeriesFooter1AddOriginalFilename(sw.b, filenamePosition)
    ttgstream.SeriesFooter1AddSourceSha1(sw.b, sha1Position)
    ttgstream.SeriesFooter1AddDataFnv1aChecksum(sw.b, sm.DataFnv1aChecksum())
    sfPosition := ttgstream.SeriesFooter1End(sw.b)

    sw.b.Finish(sfPosition)

    data := sw.b.FinishedBytes()

    _, err = sw.w.Write(data)
    log.PanicIf(err)

    footerVersion := uint16(1)
    err = sw.writeShadowFooter(footerVersion, FtSeriesFooter, uint16(len(data)))
    log.PanicIf(err)

    return nil
}
