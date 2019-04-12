package timetogo

import (
	"errors"
	"io"
	"time"

	"github.com/dsoprea/go-logging"
	"github.com/dsoprea/go-time-index"
)

var (
	ErrSeriesNotFound = errors.New("series not found")
)

type Index struct {
	rs         io.ReadSeeker
	sr         *StreamReader
	seriesInfo []StreamIndexedSequenceInfo
	intervals  timeindex.TimeIntervalSlice
}

func NewIndex(rs io.ReadSeeker) (index *Index, err error) {
	sr := NewStreamReader(rs)

	// Put us on the trailing NUL byte.
	err = sr.Reset()
	log.PanicIf(err)

	streamFooter, _, _, err := sr.readStreamFooter()
	log.PanicIf(err)

	seriesInfo := streamFooter.Series()

	intervals := make(timeindex.TimeIntervalSlice, 0)
	for _, sisi := range seriesInfo {
		intervals =
			intervals.Add(
				sisi.HeadRecordTime(),
				sisi.TailRecordTime(),
				sisi)
	}

	index = &Index{
		rs:         rs,
		sr:         sr,
		seriesInfo: seriesInfo,
		intervals:  intervals,
	}

	return index, nil
}

func (index *Index) GetWithTimestamp(timestamp time.Time) (matched []StreamIndexedSequenceInfo, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	intervals := index.intervals.SearchAndReturn(timestamp)
	matched = make([]StreamIndexedSequenceInfo, 0)
	for _, interval := range intervals {
		for _, data := range interval.Items {
			matched = append(matched, data.(StreamIndexedSequenceInfo))
		}
	}

	return matched, nil
}

// TODO(dustin): !! Rename StreamIndexedSequenceInfo to StreamIndexedSeriesInfo
