package timetogo

import (
	"fmt"
	"io"
	"os"

	"io/ioutil"

	"github.com/dsoprea/go-logging"
	"github.com/randomingenuity/go-utility/filesystem"
)

var (
	updaterLogger = log.NewLogger("timetogo.updater")
)

type seriesIndexKey struct {
	uuid       string
	sourceSha1 string
}

// Truncater is a type that knows how to truncate its bytes stream. This will
// frequently be paired with a `io.ReadWriteSeeker`.
type Truncater interface {
	Truncate(size int64) error
}

// Updater manages syncing what the caller has with what is stored.
//
// 1. Copy all unchanged series, in their current sequence, from where they
//    currently are to the front of the file.
//
// 2. Use the data-writer interface to generate a serialized representation of
//    the changed/new ones. Place them at the end in the order that they were
// 	  stored before (those that are being updated) or in the order they were
//    added (the new ones).
type Updater struct {
	rws io.ReadWriteSeeker
	it  *Iterator
	sr  *StreamReader
	sb  *StreamBuilder

	br *rifs.BouncebackReader

	seriesDataWriter interface{}
	newSeries        []SeriesFooter

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

// NewUpdater returns a new `Updater` struct.
func NewUpdater(rws io.ReadWriteSeeker, seriesDataWriter interface{}) *Updater {
	sr := NewStreamReader(rws)

	br, err := rifs.NewBouncebackReader(rws)
	log.PanicIf(err)

	bw, err := rifs.NewBouncebackWriter(rws)
	log.PanicIf(err)

	sb := NewStreamBuilder(bw)

	dataPresent := true
	it, err := NewIterator(sr)
	if err != nil {
		if err == io.EOF {
			dataPresent = false
		} else {
			log.Panic(err)
		}
	}

	// Read existing series data. This does N seeks through the stream, but is
	// otherwise an efficient operation.

	knownSeriesIndex := make(map[seriesIndexKey]currentPersistedSeries)

	if dataPresent == true {
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

		// Now that we've enumerated the series, go back to the front of the stream
		// so that we're in a position to begin stepping forward.
		_, err = bw.Seek(0, os.SEEK_SET)
		log.PanicIf(err)
	}

	newSeries := make([]SeriesFooter, 0)

	return &Updater{
		rws:              rws,
		it:               it,
		sr:               sr,
		sb:               sb,
		br:               br,
		seriesDataWriter: seriesDataWriter,
		knownSeriesIndex: knownSeriesIndex,
		newSeries:        newSeries,
	}
}

// SetStructureLogging enables/disables structure tracking.
func (updater *Updater) SetStructureLogging(flag bool) {
	updater.sb.StreamWriter().SetStructureLogging(flag)
}

// Structure returns the `StreamStructure` struct (if enabled).
func (updater *Updater) Structure() *StreamStructure {
	return updater.sb.StreamWriter().Structure()
}

// AddSeries queues a series to be added. It's not actually written until
// Write() is called.
func (updater *Updater) AddSeries(seriesFooter SeriesFooter) {
	defer func() {
		if state := recover(); state != nil {
			err := log.Wrap(state.(error))
			log.Panic(err)
		}
	}()

	updater.newSeries = append(updater.newSeries, seriesFooter)
}

// appendNewSeries writes the given series out to the stream.
func (updater *Updater) appendNewSeries(seriesFooter SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add test.

	if updater.seriesDataWriter == nil {
		log.Panicf("data needed for series [%s] but no data-writer was provided", seriesFooter.Uuid())
	}

	updaterLogger.Debugf(nil, "appendNewSeries: Adding new series [%s].", seriesFooter.Uuid())

	seriesFooter.TouchUpdatedTime()

	err = updater.sb.AddSeries(updater.seriesDataWriter, seriesFooter)
	log.PanicIf(err)

	return nil
}

// copyForwardSeries adds the series but copies the data from a later position
// in the file.
func (updater *Updater) copyForwardSeries(existingFilePosition int64, seriesFooter SeriesFooter) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add test.

	_, err = updater.br.Seek(existingFilePosition, os.SEEK_SET)
	log.PanicIf(err)

	dataSize := int64(seriesFooter.BytesLength())

	lr := io.LimitReader(updater.br, dataSize)
	rc := ioutil.NopCloser(lr)

	updaterLogger.Debugf(nil, "copyForwardSeries: Copying-forward existing series [%s] from position (%d).", seriesFooter.Uuid(), existingFilePosition)

	err = updater.sb.AddSeries(rc, seriesFooter)
	log.PanicIf(err)

	return nil
}

// addExistingSeries either reuses or appends/overwrites an existing series
// depending on whether it's unchanged and if the existing data has already been
// overwritten.
func (updater *Updater) addExistingSeries(seriesFooter SeriesFooter, cps currentPersistedSeries, currentSequencePosition int, anyChanges *bool) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	// TODO(dustin): !! Add test.

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
		// The series is already in the stream in the same place (and unchanged,
		// or this function would've never been called).

		updaterLogger.Debugf(nil, "addExistingSeries: Skipping over existing series [%s].", seriesFooter.Uuid())

		err := updater.sb.AddSeriesNoWrite(existingFilePosition, existingTotalSeriesSize, seriesFooter)
		log.PanicIf(err)
	} else if int64(existingSeriesPosition) > updater.sb.NextOffset() {
		// The series is already in the stream, but past the current position.

		*anyChanges = true

		updaterLogger.Debugf(nil, "addExistingSeries: Copying-forward series [%s].", seriesFooter.Uuid())

		err := updater.copyForwardSeries(existingFilePosition, existingSeriesFooter)
		log.PanicIf(err)
	} else {
		// The series is either not already in the stream or changed from the
		// one that's present.

		*anyChanges = true

		updaterLogger.Debugf(nil, "addExistingSeries: We have previously encountered changes. Adding series [%s] as new.", seriesFooter.Uuid())

		// We use the *existing* footer because the data is supposed to be
		// identical and, so far, looks identical, and we want to be very
		// sure that the caller doesn't introduce changes.
		err := updater.appendNewSeries(existingSeriesFooter)
		log.PanicIf(err)
	}

	return nil
}

// UpdateStats keeps a tally of various operations.
type UpdateStats struct {
	Skips int
	Adds  int
	Drops int
}

func (us UpdateStats) String() string {
	return fmt.Sprintf("UpdateStats<SKIPS=(%d) ADDS=(%d) DROPS=(%d)>", us.Skips, us.Adds, us.Drops)
}

// Write executes the queued changes.
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

	hitsExisting := 0
	for i, seriesFooter := range updater.newSeries {
		sik := updateSeriesIndexingKey(seriesFooter)
		if cps, isExisting := updater.knownSeriesIndex[sik]; isExisting == false {
			continue
		} else {
			err := updater.addExistingSeries(seriesFooter, cps, i, &anyChanges)
			log.PanicIf(err)

			sequencePosition++
			stats.Skips++
			hitsExisting++
		}
	}

	stats.Drops = len(updater.knownSeriesIndex) - hitsExisting

	// Now, add all of the new/changed series to the back.

	for _, seriesFooter := range updater.newSeries {
		sik := updateSeriesIndexingKey(seriesFooter)
		if _, isExisting := updater.knownSeriesIndex[sik]; isExisting == true {
			continue
		}

		err := updater.appendNewSeries(seriesFooter)
		log.PanicIf(err)

		sequencePosition++
		stats.Adds++
		anyChanges = true
	}

	noopStats := UpdateStats{0, 0, 0}
	if stats == noopStats {
		updaterLogger.Debugf(nil, "No changes were made in the update. Not updating the stream footer.")

		// Seek to the end so that we can still discover and get the length.

		streamFooterHeadBytePosition, err := updater.rws.Seek(0, os.SEEK_CUR)
		log.PanicIf(err)

		err = updater.sr.Reset()
		log.PanicIf(err)

		_, _, footerBytes, footerOffset, err := updater.sr.readOneFooter()
		log.PanicIf(err)

		streamFooterHeadBytePosition = streamFooterHeadBytePosition
		footerBytes = footerBytes
		footerOffset = footerOffset

		if streamFooterHeadBytePosition != footerOffset {
			log.Panicf("after the no-op update, we expected to be on the head byte of the stream footer but weren't: (%d) != (%d)", streamFooterHeadBytePosition, footerOffset)
		}

		totalSize = int(footerOffset) + len(footerBytes) + ShadowFooterSize
	} else {
		totalSize, err = updater.sb.Finish()
		log.PanicIf(err)

		if truncater, ok := updater.rws.(Truncater); ok == true {
			updaterLogger.Debugf(nil, "Underlying RWS is also a truncater. Truncating stream to right size after update.")

			err = truncater.Truncate(int64(totalSize))
			log.PanicIf(err)
		}
	}

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
