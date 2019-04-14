package timetogo

import (
	"fmt"
	// "sort"
	"strings"

	"github.com/dsoprea/go-logging"
)

type MilestoneType string

const (
	MtSeriesDataHeadByte   MilestoneType = "series_data_head_byte"
	MtFooterHeadByte       MilestoneType = "footer_head_byte"
	MtSeriesFooterHeadByte MilestoneType = "series_footer_head_byte"
	MtStreamFooterHeadByte MilestoneType = "stream_footer_head_byte"
	MtBoundaryMarker       MilestoneType = "boundary_marker"
	MtShadowFooterHeadByte MilestoneType = "shadow_footer_head_byte"
	MtSeriesFooterDecoded  MilestoneType = "series_footer_decoded"
	MtStreamFooterDecoded  MilestoneType = "stream_footer_decoded"
)

type ScopeType int

const (
	StSeries ScopeType = iota
	StStream ScopeType = iota
	StMisc   ScopeType = iota
)

var (
	ScopeTypePhrases = map[ScopeType]string{
		StSeries: "series",
		StStream: "stream",
		StMisc:   "misc",
	}
)

type StreamStructureOffsetInfo struct {
	Offset int64

	MilestoneType MilestoneType

	ScopeType ScopeType

	// SeriesUuid is a UUID of the series, if this offset refers to a series.
	SeriesUuid string

	Comment string
}

type StreamStructure struct {
	milestones []StreamStructureOffsetInfo
}

func NewStreamStructure() *StreamStructure {
	milestones := make([]StreamStructureOffsetInfo, 0)

	return &StreamStructure{
		milestones: milestones,
	}
}

// TODO(dustin): !! These jumble the data, apparently, and it looks better just looking at the entries in insert-order.
//
// // Len is the number of elements in the collection.
// func (ss *StreamStructure) Len() int {
// 	return len(ss.milestones)
// }

// // Less reports whether the element with
// // index i should sort before the element with index j.
// func (ss *StreamStructure) Less(i, j int) bool {
// 	return ss.milestones[i].Offset < ss.milestones[j].Offset || ss.milestones[i].Offset == ss.milestones[j].Offset && ss.milestones[i].MilestoneType < ss.milestones[j].MilestoneType
// }

// // Swap swaps the elements with indexes i and j.
// func (ss *StreamStructure) Swap(i, j int) {
// 	ss.milestones[i].Offset, ss.milestones[j].Offset = ss.milestones[j].Offset, ss.milestones[i].Offset
// }

func (ss *StreamStructure) Dump() {
	if ss.milestones == nil {
		log.Panicf("milestones not collected")
	}

	// sort.Sort(ss)

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

		fmt.Printf("%s  MT %-30s  SCOPE %-7s  UUID %-40s  COMM %-s\n", offsetPhrase, milestone.MilestoneType, ScopeTypePhrases[milestone.ScopeType], milestone.SeriesUuid, milestone.Comment)

		lastOffset = milestone.Offset
	}

	fmt.Printf("\n")
}

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

func (ss *StreamStructure) Milestones() []StreamStructureOffsetInfo {
	return ss.milestones
}

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
