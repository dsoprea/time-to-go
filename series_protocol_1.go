package timetogo

import (
    "fmt"
    "time"

    "github.com/dsoprea/go-logging"

    "github.com/dsoprea/time-to-go/protocol/ttgstream"
)

// SeriesFooter1 describes the data in a single series. Version 1.
type SeriesFooter1 struct {
    // The timestamp of the first record
    headRecordTime time.Time

    // The timestamp of the last record
    tailRecordTime time.Time

    // The number of bytes occupied on-disk
    bytesLength uint64

    // The number of records in the list
    recordCount uint64

    // The filename of the source-data
    originalFilename string

    // SHA1 of the raw source-data; can be used to determine if the source-data has changed
    sourceSha1 []byte

    // FNV-1a checksum of the time-series data on-disk
    dataFnv1aChecksum uint32
}

// NewSeriesFooter1 returns a series footer structure. Version 1.
func NewSeriesFooter1(headRecordTime time.Time, tailRecordTime time.Time, bytesLength, recordCount uint64, originalFilename string, sourceSha1 []byte, dataFnv1aChecksum uint32) *SeriesFooter1 {
    return &SeriesFooter1{
        headRecordTime:    headRecordTime.UTC(),
        tailRecordTime:    tailRecordTime.UTC(),
        bytesLength:       bytesLength,
        recordCount:       recordCount,
        originalFilename:  originalFilename,
        sourceSha1:        sourceSha1,
        dataFnv1aChecksum: dataFnv1aChecksum,
    }
}

func NewSeriesFooter1FromEncoded(footerBytes []byte) (sf *SeriesFooter1) {
    sfEncoded := ttgstream.GetRootAsSeriesFooter1(footerBytes, 0)

    headRecordTime := time.Unix(int64(sfEncoded.HeadRecordEpoch()), 0).In(time.UTC)
    tailRecordTime := time.Unix(int64(sfEncoded.TailRecordEpoch()), 0).In(time.UTC)

    sf = &SeriesFooter1{
        headRecordTime:    headRecordTime,
        tailRecordTime:    tailRecordTime,
        bytesLength:       sfEncoded.BytesLength(),
        recordCount:       sfEncoded.RecordCount(),
        originalFilename:  string(sfEncoded.OriginalFilename()),
        dataFnv1aChecksum: sfEncoded.DataFnv1aChecksum(),
        sourceSha1:        sfEncoded.SourceSha1(),
    }

    return sf
}

func (sf *SeriesFooter1) String() string {
    return fmt.Sprintf("SeriesFooter1<HEAD=[%s] TAIL=[%s] BYTES=(%d) COUNT=(%d) FILENAME=[%s] CHECKSUM=(%d) SOURCE-SHA1=[%20x]>",
        sf.headRecordTime,
        sf.tailRecordTime,
        sf.bytesLength,
        sf.recordCount,
        sf.originalFilename,
        sf.dataFnv1aChecksum,
        sf.sourceSha1)
}

func (sf *SeriesFooter1) Version() SeriesFooterVersion {
    return SeriesFooterVersion1
}

func (sf *SeriesFooter1) HeadRecordTime() time.Time {
    return sf.headRecordTime
}

func (sf *SeriesFooter1) TailRecordTime() time.Time {
    return sf.tailRecordTime
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

// writeFooter1 will write the footer for a series. When this returns, we'll be
// in the position following the final NUL byte.
func (sw *StreamWriter) writeSeriesFooter1(sf SeriesFooter) (err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // TDO(dustin): !! We also need to finish off the stream with a metadata struct.

    sw.b.Reset()

    filenamePosition := sw.b.CreateString(sf.OriginalFilename())
    sha1Position := sw.b.CreateByteString(sf.SourceSha1())

    ttgstream.SeriesFooter1Start(sw.b)
    ttgstream.SeriesFooter1AddHeadRecordEpoch(sw.b, uint64(sf.HeadRecordTime().Unix()))
    ttgstream.SeriesFooter1AddTailRecordEpoch(sw.b, uint64(sf.TailRecordTime().Unix()))
    ttgstream.SeriesFooter1AddBytesLength(sw.b, sf.BytesLength())
    ttgstream.SeriesFooter1AddRecordCount(sw.b, sf.RecordCount())
    ttgstream.SeriesFooter1AddOriginalFilename(sw.b, filenamePosition)
    ttgstream.SeriesFooter1AddSourceSha1(sw.b, sha1Position)
    ttgstream.SeriesFooter1AddDataFnv1aChecksum(sw.b, sf.DataFnv1aChecksum())
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
