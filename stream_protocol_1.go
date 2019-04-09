package timetogo

import (
    "fmt"
    "time"

    "github.com/dsoprea/go-logging"
    "github.com/google/flatbuffers/go"

    "github.com/dsoprea/time-to-go/protocol/ttgstream"
)

var (
    streamLogger1 = log.NewLogger("timetogo.stream_protocol_1")
)

// StreamIndexedSequenceInfo1 briefly describes all series.
type StreamIndexedSequenceInfo1 struct {
    // headRecordTime is the timestamp of the first record
    headRecordTime time.Time

    // tailRecordTime is the timestamp of the last record
    tailRecordTime time.Time

    // originalFilename is the filename of the source-data
    originalFilename string

    // absolutePosition is the absolute position of the boundary marker (NUL)
    absolutePosition int64
}

// NewStreamIndexedSequenceInfo1 returns a sequence-info structure.
func NewStreamIndexedSequenceInfo1(headRecordTime, tailRecordTime time.Time, originalFilename string, absolutePosition int64) *StreamIndexedSequenceInfo1 {
    return &StreamIndexedSequenceInfo1{
        headRecordTime:   headRecordTime.UTC(),
        tailRecordTime:   tailRecordTime.UTC(),
        originalFilename: originalFilename,
        absolutePosition: absolutePosition,
    }
}

// HeadRecordTime is the timestamp of the first record
func (sisi StreamIndexedSequenceInfo1) HeadRecordTime() time.Time {
    return sisi.headRecordTime
}

// TailRecordTime is the timestamp of the last record
func (sisi StreamIndexedSequenceInfo1) TailRecordTime() time.Time {
    return sisi.tailRecordTime
}

// OriginalFilename is the filename of the source-data
func (sisi StreamIndexedSequenceInfo1) OriginalFilename() string {
    return sisi.originalFilename
}

// AbsolutePosition is the absolute position of the boundary marker (NUL)
func (sisi StreamIndexedSequenceInfo1) AbsolutePosition() int64 {
    return sisi.absolutePosition
}

func (sisi StreamIndexedSequenceInfo1) String() string {
    return fmt.Sprintf("StreamIndexedSequenceInfo1<HEAD=[%s] TAIL=[%s] FILENAME=[%s] POSITION=(%d)", sisi.headRecordTime, sisi.tailRecordTime, sisi.originalFilename, sisi.absolutePosition)
}

// writeStreamFooter writes a block of data that describes the entire stream.
func (sw *StreamWriter) writeStreamFooter(sequences []StreamIndexedSequenceInfo) (size int, err error) {
    defer func() {
        if state := recover(); state != nil {
            if message, ok := state.(string); ok == true {
                err = log.Errorf(message)
            } else {
                err = log.Wrap(state.(error))
            }
        }
    }()

    sw.b.Reset()

    sisiOffsets := make([]flatbuffers.UOffsetT, len(sequences))
    for i, sisi := range sequences {
        filenamePosition := sw.b.CreateString(sisi.OriginalFilename())

        ttgstream.StreamIndexedSequenceInfoStart(sw.b)
        ttgstream.StreamIndexedSequenceInfoAddHeadRecordEpoch(sw.b, uint64(sisi.HeadRecordTime().Unix()))
        ttgstream.StreamIndexedSequenceInfoAddTailRecordEpoch(sw.b, uint64(sisi.TailRecordTime().Unix()))
        ttgstream.StreamIndexedSequenceInfoAddOriginalFilename(sw.b, filenamePosition)
        ttgstream.StreamIndexedSequenceInfoAddAbsolutePosition(sw.b, sisi.AbsolutePosition())

        sisiOffset := ttgstream.StreamIndexedSequenceInfoEnd(sw.b)
        sisiOffsets[i] = sisiOffset
    }

    seriesCount := len(sequences)
    ttgstream.StreamFooter1StartSeriesVector(sw.b, seriesCount)

    for i := len(sisiOffsets) - 1; i >= 0; i-- {
        sisiOffset := sisiOffsets[i]
        sw.b.PrependUOffsetT(sisiOffset)
    }

    seriesVectorOffset := sw.b.EndVector(seriesCount)

    ttgstream.StreamFooter1Start(sw.b)

    ttgstream.StreamFooter1AddSeries(sw.b, seriesVectorOffset)

    sfPosition := ttgstream.StreamFooter1End(sw.b)
    sw.b.Finish(sfPosition)

    data := sw.b.FinishedBytes()

    cw, isCounter := sw.w.(*CountingWriter)

    if isCounter == true {
        streamLogger1.Debugf(nil, "Writing (%d) bytes for stream footer at (%d).", len(data), cw.Position())
    } else {
        streamLogger1.Debugf(nil, "Writing (%d) bytes for stream footer.", len(data))
    }

    _, err = sw.w.Write(data)
    log.PanicIf(err)

    footerVersion := uint16(1)
    shadowSize, err := sw.writeShadowFooter(footerVersion, FtStreamFooter, uint16(len(data)))
    log.PanicIf(err)

    size = len(data) + shadowSize
    return size, nil
}

type StreamFooter1 struct {
    series []StreamIndexedSequenceInfo
}

func (sf *StreamFooter1) String() string {
    return fmt.Sprintf("StreamFooter1<COUNT=(%d)>", len(sf.Series()))
}

func (sf *StreamFooter1) Series() []StreamIndexedSequenceInfo {
    return sf.series
}

func NewStreamFooter1FromEncoded(footerBytes []byte) (sf StreamFooter, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    sfEncoded := ttgstream.GetRootAsStreamFooter1(footerBytes, 0)

    seriesCount := sfEncoded.SeriesLength()
    series := make([]StreamIndexedSequenceInfo, seriesCount)
    for i := 0; i < seriesCount; i++ {
        sisiEncoded := ttgstream.StreamIndexedSequenceInfo{}
        found := sfEncoded.Series(&sisiEncoded, i)
        if found == false {
            log.Panicf("could not find series (%d) info in stream info", i)
        }

        headRecordTime := time.Unix(int64(sisiEncoded.HeadRecordEpoch()), 0).In(time.UTC)
        tailRecordTime := time.Unix(int64(sisiEncoded.TailRecordEpoch()), 0).In(time.UTC)

        sisi := &StreamIndexedSequenceInfo1{
            headRecordTime:   headRecordTime,
            tailRecordTime:   tailRecordTime,
            originalFilename: string(sisiEncoded.OriginalFilename()),
            absolutePosition: sisiEncoded.AbsolutePosition(),
        }

        series[i] = sisi
    }

    sf = &StreamFooter1{
        series: series,
    }

    return sf, nil
}
