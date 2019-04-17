package timetogo

import (
	"fmt"
	// "sort"
	"strings"

	"github.com/dsoprea/go-logging"
)

// MilestoneType is the name of the event-type.
type MilestoneType string

const (
	// MtSeriesDataHeadByte marks the first byte of a series' time-series data.
	MtSeriesDataHeadByte MilestoneType = "series_data_head_byte"

	// MtFooterHeadByte marks the first byte of a footer of a yet-unidentified
	// type.
	MtFooterHeadByte MilestoneType = "footer_head_byte"

	// MtSeriesFooterHeadByte marks the first byte of a series footer.
	MtSeriesFooterHeadByte MilestoneType = "series_footer_head_byte"

	// MtStreamFooterHeadByte marks the first byte of a stream footer.
	MtStreamFooterHeadByte MilestoneType = "stream_footer_head_byte"

	// MtBoundaryMarker identifies a boundary marker.
	MtBoundaryMarker MilestoneType = "boundary_marker"

	// MtShadowFooterHeadByte marks the first byte of the static shadow footer
	// that immediately follows any other type of footer.
	MtShadowFooterHeadByte MilestoneType = "shadow_footer_head_byte"

	// MtSeriesFooterDecoded marks the first byte of a series footer that has
	// been successfully decoded.
	MtSeriesFooterDecoded MilestoneType = "series_footer_decoded"

	// MtStreamFooterDecoded marks the first byte of a stream footer that has
	// been successfully decoded.
	MtStreamFooterDecoded MilestoneType = "stream_footer_decoded"
)

// ScopeType is which type of data the event applies to.
type ScopeType int

const (
	// StSeries describes milestones that pertain to series.
	StSeries ScopeType = iota

	// StStream describes milestones that pertain to streams.
	StStream ScopeType = iota

	// StMisc describes milestones that are either agnostic (not likely) or
	// could be any other scope type but there's not yet enough information
	// to tell (likely).
	StMisc ScopeType = iota
)

var (
	// ScopeTypePhrases are simple labels for the various scope types.
	ScopeTypePhrases = map[ScopeType]string{
		StSeries: "series",
		StStream: "stream",
		StMisc:   "misc",
	}
)

// StreamStructureOffsetInfo describes a single recorded milestone.
type StreamStructureOffsetInfo struct {
	Offset int64

	MilestoneType MilestoneType

	ScopeType ScopeType

	// SeriesUuid is a UUID of the series, if this offset refers to a series.
	SeriesUuid string

	Comment string
}

func (ssoi StreamStructureOffsetInfo) String() string {
	return fmt.Sprintf("StreamStructureOffsetInfo<OFFSET=(%d) MILESTONE=[%s] SCOPE=(%d) UUID=[%s] COMMENT=[%s]>", ssoi.Offset, ssoi.MilestoneType, ssoi.ScopeType, ssoi.SeriesUuid, ssoi.Comment)
}

// StreamStructure holds all of the milestones recorded for a given stream.
type StreamStructure struct {
	milestones []StreamStructureOffsetInfo
}

// NewStreamStructure returns a new `StreamStructure`.
func NewStreamStructure() *StreamStructure {
	milestones := make([]StreamStructureOffsetInfo, 0)

	return &StreamStructure{
		milestones: milestones,
	}
}

func (ss *StreamStructure) String() string {
	return fmt.Sprintf("StreamStructure<COUNT=(%d)>", len(ss.milestones))
}

// Dump prints a table with all of the recorded milestones in the order that
// they were encountered.
func (ss *StreamStructure) Dump() {
	if ss.milestones == nil {
		log.Panicf("milestones not collected")
	}

	if len(ss.milestones) == 0 {
		log.Panicf("no milestones recorded")
	}

	fmt.Printf("================\n")
	fmt.Printf("Stream Structure\n")
	fmt.Printf("================\n")
	fmt.Printf("\n")

	var lastOffset int64
	for i, milestone := range ss.milestones {
		offsetPhrase := strings.Repeat(" ", 11)
		if i == 0 || milestone.Offset != lastOffset {
			offsetPhrase = fmt.Sprintf("OFF %-7d", milestone.Offset)
		}

		comment := milestone.Comment

		// Don't print any trailing spaces. This sabotages are testable
		// examples because our editor will trim the trailing whitespace
		// and the expected output will no longer match the actual output.
		if comment != "" {
			comment = " " + comment
		}

		fmt.Printf("%s  MT %-30s  SCOPE %-7s  UUID %-40s  COMM%-s\n", offsetPhrase, milestone.MilestoneType, ScopeTypePhrases[milestone.ScopeType], milestone.SeriesUuid, comment)

		lastOffset = milestone.Offset
	}

	fmt.Printf("\n")
}

// Push records a single event.
func (ss *StreamStructure) Push(offset int64, milestoneType MilestoneType, scopeType ScopeType, seriesUuid string, comment string) {
	ssoi := StreamStructureOffsetInfo{
		Offset:        offset,
		MilestoneType: milestoneType,
		ScopeType:     scopeType,
		SeriesUuid:    seriesUuid,
		Comment:       comment,
	}

	ss.milestones = append(ss.milestones, ssoi)
}

// Milestones returns all recorded milestones.
func (ss *StreamStructure) Milestones() []StreamStructureOffsetInfo {
	return ss.milestones
}

// StreamMilestones returns all stream-specific milestones.
func (ss *StreamStructure) StreamMilestones() []StreamStructureOffsetInfo {
	streamMilestones := make([]StreamStructureOffsetInfo, 0)
	for _, ssoi := range ss.milestones {
		if ssoi.ScopeType != StStream {
			continue
		}

		streamMilestones = append(streamMilestones, ssoi)
	}

	return streamMilestones
}

// SeriesMilestones returns all series-specific milestones,optionally filtering
// for a specific one. Returned as a flat list.
func (ss *StreamStructure) SeriesMilestones(uuid string) []StreamStructureOffsetInfo {
	seriesMilestones := make([]StreamStructureOffsetInfo, 0)
	for _, ssoi := range ss.milestones {
		if ssoi.ScopeType != StSeries {
			continue
		}

		seriesMilestones = append(seriesMilestones, ssoi)
	}

	// If we were asked for a specific UUID.
	if uuid != "" {
		currentUuid := ""
		currentSeries := make([]StreamStructureOffsetInfo, 0)
		for _, ssoi := range seriesMilestones {
			if ssoi.MilestoneType == MtBoundaryMarker {
				// We encountered the next series. If the last series was what
				// we were looking for, return all of those milestones.
				if currentUuid == uuid {
					return currentSeries
				}

				currentSeries = make([]StreamStructureOffsetInfo, 0)
				currentUuid = ""
			} else if ssoi.MilestoneType == MtSeriesFooterDecoded && ssoi.SeriesUuid == uuid {
				currentUuid = uuid
			}

			currentSeries = append(currentSeries, ssoi)
		}

		// We ran out of data. If the last series was what we were looking for,
		// return all of those milestones.
		if currentUuid == uuid {
			return currentSeries
		}

		// Not found.
		return nil
	}

	return seriesMilestones
}

// AllSeriesMilestones returns a map of all recorded series.
func (ss *StreamStructure) AllSeriesMilestones() (milestoneIndex map[string][]StreamStructureOffsetInfo) {
	milestoneIndex = make(map[string][]StreamStructureOffsetInfo)

	for _, ssoi := range ss.milestones {
		if ssoi.ScopeType != StSeries {
			continue
		}

		currentUuid := ""
		currentSeries := make([]StreamStructureOffsetInfo, 0)

		flush := func() {
			if currentUuid == "" {
				log.Panicf("current UUID is empty")
			}

			milestoneIndex[currentUuid] = currentSeries

			currentSeries = make([]StreamStructureOffsetInfo, 0)
			currentUuid = ""
		}

		if ssoi.MilestoneType == MtBoundaryMarker {
			// If we're not just starting out.
			if currentUuid != "" {
				flush()
			}
		} else if ssoi.MilestoneType == MtSeriesFooterDecoded && ssoi.SeriesUuid == currentUuid {
			currentUuid = ssoi.SeriesUuid
		}

		currentSeries = append(currentSeries, ssoi)

		if currentUuid != "" {
			flush()
		}
	}

	return milestoneIndex
}

// MilestonesWithFilter returns all milestones, optionally applying a filter.
func (ss *StreamStructure) MilestonesWithFilter(milestoneType string, scopeType int) []StreamStructureOffsetInfo {
	milestones := make([]StreamStructureOffsetInfo, 0)
	for _, ssoi := range ss.milestones {
		if scopeType != -1 && ssoi.ScopeType != ScopeType(scopeType) {
			continue
		}

		if milestoneType != "" && ssoi.MilestoneType != MilestoneType(milestoneType) {
			continue
		}

		milestones = append(milestones, ssoi)
	}

	return milestones
}
