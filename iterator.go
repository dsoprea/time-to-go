package timetogo

import (
    "io"
    "os"

    "github.com/dsoprea/go-logging"
)

// Iterator efficiently steps backwards through the series in a stream in order.
type Iterator struct {
    rs            io.ReadSeeker
    sr            *StreamReader
    seriesInfo    []StreamIndexedSequenceInfo
    currentSeries int
}

// Count returns the number of series in the stream.
func (it *Iterator) Count() int {
    return len(it.seriesInfo)
}

// Current returns the number of the series that we're currently on. This
// decrements after each call and returns less than zero on EOF.
func (it *Iterator) Current() int {
    return it.currentSeries
}

// SeriesInfo efficiently returns summary information for one of the series in
// the stream.
func (it *Iterator) SeriesInfo(i int) StreamIndexedSequenceInfo {
    return it.seriesInfo[i]
}

func NewIterator(rs io.ReadSeeker) (it *Iterator, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    // Put us on the trailing NUL byte.
    _, err = rs.Seek(-1, os.SEEK_END)
    log.PanicIf(err)

    sr := NewStreamReader(rs)

    streamFooter, _, _, err := sr.readStreamFooter()
    log.PanicIf(err)

    seriesInfo := streamFooter.Series()

    it = &Iterator{
        rs:            rs,
        sr:            sr,
        seriesInfo:    seriesInfo,
        currentSeries: len(seriesInfo) - 1,
    }

    return it, nil
}

func (it *Iterator) Iterate() (seriesFooter SeriesFooter, seriesData []byte, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    if it.currentSeries < 0 {
        return nil, nil, io.EOF
    }

    sisi := it.seriesInfo[it.currentSeries]
    it.currentSeries--

    seriesFooter, seriesData, _, err = it.sr.readSeriesWithIndexedInfo(sisi)
    log.PanicIf(err)

    return seriesFooter, seriesData, nil
}
