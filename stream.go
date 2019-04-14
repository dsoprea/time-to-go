package timetogo

import (
	"time"
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
