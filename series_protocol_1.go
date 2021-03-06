package timetogo

import (
	"fmt"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/google/uuid"

	"github.com/dsoprea/time-to-go/protocol/ttgstream"
)

var (
	seriesProtocol1Logger = log.NewLogger("timetogo.series_protocol_1")
)

// SeriesFooter1 describes the data in a single series. Version 1.
type SeriesFooter1 struct {
	// uuid is a unique string that uniquely identifies this series in the
	// stream.
	uuid string

	// headRecordTime is the timestamp of the first record
	headRecordTime time.Time

	// tailRecordTime is the timestamp of the last record
	tailRecordTime time.Time

	// bytesLength is the number of bytes occupied on-disk
	bytesLength uint64

	// createdTime is the timestamp of the first write of this series
	createdTime time.Time

	// updatedTime is the timestamp of the last update
	updatedTime time.Time

	// recordCount is the number of records in the list
	recordCount uint64

	// sourceSha1 is the SHA1 of the raw source-data; can be used to determine
	// if the source-data has changed
	sourceSha1 []byte

	// dataFnv1aChecksum FNV-1a checksum of the time-series data on-disk
	dataFnv1aChecksum uint32
}

// TODO(dustin): !! We should be versioning the API and the data structure. They shouldn't necessarily be the same thing. We might change the on-disk representation without touching the API call that is called from external.

// NewSeriesFooter1 returns a series footer structure. Version 1. The checksum
// will be populated on write.
func NewSeriesFooter1(headRecordTime time.Time, tailRecordTime time.Time, recordCount uint64, sourceSha1 []byte) *SeriesFooter1 {
	uuid := uuid.New().String()

	now := time.Now().UTC()
	now = now.Add(-time.Nanosecond * time.Duration(now.Nanosecond()))

	return &SeriesFooter1{
		uuid:           uuid,
		headRecordTime: headRecordTime.UTC(),
		tailRecordTime: tailRecordTime.UTC(),
		recordCount:    recordCount,
		createdTime:    now,
		updatedTime:    now,
		sourceSha1:     sourceSha1,
	}
}

func (sf *SeriesFooter1) SetBytesLength(bytesLength uint64) {
	sf.bytesLength = bytesLength
}

// NewSeriesFooter1FromEncoded returns a series footer struct (version 1). The
// checksum that was recorded during the write will be populated.
func NewSeriesFooter1FromEncoded(footerBytes []byte) (sf *SeriesFooter1, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	sfEncoded := ttgstream.GetRootAsSeriesFooter1(footerBytes, 0)

	headRecordTime := time.Unix(int64(sfEncoded.HeadRecordEpoch()), 0).In(time.UTC)
	tailRecordTime := time.Unix(int64(sfEncoded.TailRecordEpoch()), 0).In(time.UTC)
	createdTime := time.Unix(int64(sfEncoded.CreatedEpoch()), 0).In(time.UTC)
	updatedTime := time.Unix(int64(sfEncoded.UpdatedEpoch()), 0).In(time.UTC)

	sf = &SeriesFooter1{
		uuid:              string(sfEncoded.Uuid()),
		headRecordTime:    headRecordTime,
		tailRecordTime:    tailRecordTime,
		bytesLength:       sfEncoded.BytesLength(),
		createdTime:       createdTime,
		updatedTime:       updatedTime,
		recordCount:       sfEncoded.RecordCount(),
		sourceSha1:        sfEncoded.SourceSha1(),
		dataFnv1aChecksum: sfEncoded.DataFnv1aChecksum(),
	}

	return sf, nil
}

// TouchUpdatedTime bumps the updated-time field.
func (sf *SeriesFooter1) TouchUpdatedTime() {
	sf.updatedTime = time.Now().UTC()
	sf.updatedTime = sf.updatedTime.Add(-time.Nanosecond * time.Duration(sf.updatedTime.Nanosecond()))
}

func (sf *SeriesFooter1) String() string {
	return fmt.Sprintf("SeriesFooter1<UUID=[%s] HEAD=[%s] TAIL=[%s] BYTES=(%d) COUNT=(%d) CREATED-AT=[%v] UPDATED-AT=[%v] SOURCE-SHA1=[%20x] CHECKSUM=(%d)>",
		sf.uuid,
		sf.headRecordTime,
		sf.tailRecordTime,
		sf.bytesLength,
		sf.recordCount,
		sf.createdTime,
		sf.updatedTime,
		sf.sourceSha1,
		sf.dataFnv1aChecksum)
}

// Version returns the series-protocol represented by this struct.
func (sf *SeriesFooter1) Version() SeriesFooterVersion {
	return SeriesFooterVersion1
}

// Uuid returns the UUID of the series.
func (sf *SeriesFooter1) Uuid() string {
	return sf.uuid
}

// HeadRecordTime is the earliest timestamp represented in the series data.
func (sf *SeriesFooter1) HeadRecordTime() time.Time {
	return sf.headRecordTime
}

// TailRecordTime is the latest timestamp represented in the series data.
func (sf *SeriesFooter1) TailRecordTime() time.Time {
	return sf.tailRecordTime
}

// BytesLength is the number of bytes of series data.
func (sf *SeriesFooter1) BytesLength() uint64 {
	return sf.bytesLength
}

// RecordCount is the number of records in the series-data.
func (sf *SeriesFooter1) RecordCount() uint64 {
	return sf.recordCount
}

// CreatedTime is the timestamp of the first write of this series
func (sf *SeriesFooter1) CreatedTime() time.Time {
	return sf.createdTime
}

// UpdatedTime is the timestamp of the last update
func (sf *SeriesFooter1) UpdatedTime() time.Time {
	return sf.updatedTime
}

// SourceSha1 is the SHA1 of the original data.
func (sf *SeriesFooter1) SourceSha1() []byte {
	return sf.sourceSha1
}

// DataFnv1aChecksum is the FNV-1a checksum of the original data. This is set
// and checked automatically, though the result of the check is returned to the
// caller rather than being enforced by us.
func (sf *SeriesFooter1) DataFnv1aChecksum() uint32 {
	return sf.dataFnv1aChecksum
}

// writeFooter1 will write the footer for a series. When this returns, we'll be
// in the position following the final NUL byte.
func (sw *StreamWriter) writeSeriesFooter1(sf SeriesFooter, fnvChecksum uint32) (size int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	sw.b.Reset()

	uuidPosition := sw.b.CreateString(sf.Uuid())
	sha1Position := sw.b.CreateByteString(sf.SourceSha1())

	ttgstream.SeriesFooter1Start(sw.b)
	ttgstream.SeriesFooter1AddUuid(sw.b, uuidPosition)
	ttgstream.SeriesFooter1AddHeadRecordEpoch(sw.b, uint64(sf.HeadRecordTime().Unix()))
	ttgstream.SeriesFooter1AddTailRecordEpoch(sw.b, uint64(sf.TailRecordTime().Unix()))
	ttgstream.SeriesFooter1AddBytesLength(sw.b, sf.BytesLength())
	ttgstream.SeriesFooter1AddRecordCount(sw.b, sf.RecordCount())
	ttgstream.SeriesFooter1AddCreatedEpoch(sw.b, uint64(sf.CreatedTime().Unix()))
	ttgstream.SeriesFooter1AddUpdatedEpoch(sw.b, uint64(sf.UpdatedTime().Unix()))
	ttgstream.SeriesFooter1AddSourceSha1(sw.b, sha1Position)
	ttgstream.SeriesFooter1AddDataFnv1aChecksum(sw.b, fnvChecksum)
	sfPosition := ttgstream.SeriesFooter1End(sw.b)

	sw.b.Finish(sfPosition)

	data := sw.b.FinishedBytes()
	seriesProtocol1Logger.Debugf(nil, "Writing (%d) bytes for series footer.", len(data))

	n, err := sw.w.Write(data)
	log.PanicIf(err)

	err = sw.pushSeriesMilestone(-1, MtSeriesFooterHeadByte, sf.Uuid(), "")
	log.PanicIf(err)

	sw.bumpPosition(int64(n))

	footerVersion := uint16(1)
	shadowSize, err := sw.writeShadowFooter(footerVersion, FtSeriesFooter, uint16(len(data)))
	log.PanicIf(err)

	size = len(data) + shadowSize
	return size, nil
}
