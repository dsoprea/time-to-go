package timetogo

import (
	"io"

	"github.com/dsoprea/go-logging"
)

type SerializeTimeSeriesDataGetter interface {
	GetSerializeTimeSeriesData(seriesFooter SeriesFooter) (rc io.ReadCloser, err error)
}

type seriesIndexKey struct {
	uuid       string
	sourceSha1 string
}

// Updater manages syncing what the caller has with what is stored.
//
// 1. Copy all unchanged series, in their current sequence, from where they
//    currently are to the front of the file.
//
// 2. Use the getter interface to generate a serialized representation of the
//    changed/new ones. Place them at the end in the order that they were stored
//    before (those that are being updated) or in the order they were added (the
//    new ones).
type Updater struct {
	rws io.ReadWriteSeeker
	it  *Iterator
	sr  *StreamReader
	sw  *StreamWriter
	sb  *StreamBuilder

	getter    SerializeTimeSeriesDataGetter
	newSeries []SeriesFooter

	knownSeriesIndex map[seriesIndexKey]currentPersistedSeries
}

type currentPersistedSeries struct {
	SeriesPosition int
	FilePosition   int64
	SeriesFooter   SeriesFooter

	// TotalSeriesSize is the size of the data plus the size of the footer,
	// shadow footer, and boundary byte.
	TotalSeriesSize int
}

func NewUpdater(rws io.ReadWriteSeeker, getter SerializeTimeSeriesDataGetter) *Updater {
	sr := NewStreamReader(rws)
	sw := NewStreamWriter(rws)
	sb := NewStreamBuilder(rws)

	it, err := NewIterator(sr)
	log.PanicIf(err)

	knownSeriesIndex := make(map[seriesIndexKey]currentPersistedSeries)

	for i := 0; i < it.Count(); i++ {
		sisi := it.SeriesInfo(i)

		seriesFooter, filePosition, totalSeriesSize, err := sr.ReadSeriesInfoWithIndexedInfo(sisi)
		log.PanicIf(err)

		sik := updateSeriesIndexingKey(seriesFooter)

		cps := currentPersistedSeries{
			SeriesPosition:  i,
			FilePosition:    filePosition,
			SeriesFooter:    seriesFooter,
			TotalSeriesSize: totalSeriesSize,
		}

		knownSeriesIndex[sik] = cps
	}

	newSeries := make([]SeriesFooter, 0)

	return &Updater{
		rws:              rws,
		it:               it,
		sr:               sr,
		sw:               sw,
		sb:               sb,
		getter:           getter,
		knownSeriesIndex: knownSeriesIndex,
		newSeries:        newSeries,
	}
}

func (updater *Updater) AddSeries(seriesFooter SeriesFooter) {
	defer func() {
		if state := recover(); state != nil {
			err := log.Wrap(state.(error))
			log.Panic(err)
		}
	}()

	updater.newSeries = append(updater.newSeries, seriesFooter)
}

func (updater *Updater) addSeries(seriesFooter SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	rc, err := updater.getter.GetSerializeTimeSeriesData(seriesFooter)
	log.PanicIf(err)

	defer rc.Close()

	err = updater.sb.AddSeries(rc, seriesFooter)
	log.PanicIf(err)

	return nil
}

func (updater *Updater) addExistingSeries(seriesFooter SeriesFooter, cps currentPersistedSeries, currentSequencePosition int, anyChanges *bool) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// This series was there to begin with. Determine whether it's in
	// exactly the same place or if there have been changes before this
	// point that will require us to move this series forward in the file.

	// This is the data that we currently have for the series. The only
	// thing that *might* change is the file-position.
	existingSeriesPosition := cps.SeriesPosition

	// existingFilePosition is the file-position of the first byte of the
	// time-series data (the time-series data is followed by the footer).
	existingFilePosition := cps.FilePosition

	existingSeriesFooter := cps.SeriesFooter
	existingTotalSeriesSize := cps.TotalSeriesSize

	// If this series and any that existed before it (if any) have, so far,
	// been identical then no copy is necessary.
	if currentSequencePosition == existingSeriesPosition && *anyChanges == false {
		err := updater.sb.AddSeriesNoWrite(existingTotalSeriesSize, seriesFooter)
		log.PanicIf(err)
	} else {
		*anyChanges = true

		// TODO(dustin): !! This needs to direct copy from where it *currently is* in theh file to where it needs to be moved to. *But*, we're gonna have to manually seek back and forther and copy it ourselves.
		// TODO(dustin): !! Once we start doing this, make sure to restore our position afterward.
		existingFilePosition = existingFilePosition

		// We use the *existing* footer because the data is supposed to be
		// identical and, so far, looks identical, and we want to be very
		// sure that the caller doesn't introduce changes.
		err = updater.addSeries(existingSeriesFooter)
		log.PanicIf(err)
	}

	return nil
}

type UpdateStats struct {
	Skips int
	Adds  int
}

func (updater *Updater) Write() (totalSize int, stats UpdateStats, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// Index the series that we're to be writing.

	newSeriesIndex := make(map[seriesIndexKey]SeriesFooter)

	for i := 0; i < len(updater.newSeries); i++ {
		seriesFooter := updater.newSeries[i]

		sik := updateSeriesIndexingKey(seriesFooter)
		newSeriesIndex[sik] = seriesFooter
	}

	// Copy the data that hasn't changed. It hasn't changed if the UUID and SHA1
	// both match.

	sequencePosition := 0
	anyChanges := false

	for i, seriesFooter := range updater.newSeries {
		sik := updateSeriesIndexingKey(seriesFooter)
		if cps, isExisting := updater.knownSeriesIndex[sik]; isExisting == false {
			continue
		} else {
			err := updater.addExistingSeries(seriesFooter, cps, i, &anyChanges)
			log.PanicIf(err)

			sequencePosition++
			stats.Skips++
		}
	}

	// Now, add all of the new/changed series to the back.

	for _, seriesFooter := range updater.newSeries {
		sik := updateSeriesIndexingKey(seriesFooter)
		if _, isExisting := updater.knownSeriesIndex[sik]; isExisting == true {
			continue
		}

		err := updater.addSeries(seriesFooter)
		log.PanicIf(err)

		sequencePosition++
		stats.Adds++
	}

	// TODO(dustin): !! The file will have to be truncated, but a bytes.Buffer object will not support this and we may have to refactor some tests.

	totalSize, err = updater.sb.Finish()
	log.PanicIf(err)

	// TODO(dustin): !! Currently, the onus is on the caller to truncate the output file to the new size (if we've shrunk). See above.
	return totalSize, stats, nil
}

// updateSeriesIndexingKey returns a key that we can use for indexing/comparing
// series.
func updateSeriesIndexingKey(seriesFooter SeriesFooter) seriesIndexKey {
	sik := seriesIndexKey{
		uuid: seriesFooter.Uuid(),

		// Use a string so that this struct is comparable.
		sourceSha1: string(seriesFooter.SourceSha1()),
	}

	return sik
}
