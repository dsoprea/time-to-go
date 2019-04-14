package timetogo

import (
	"io"

	"encoding/binary"

	"github.com/dsoprea/go-logging"
	"github.com/google/flatbuffers/go"
)

var (
	streamWriterLogger = log.NewLogger("timetogo.stream")
)

type StreamWriter struct {
	w        io.Writer
	b        *flatbuffers.Builder
	ss       *StreamStructure
	position int64
}

func NewStreamWriter(w io.Writer) *StreamWriter {
	b := flatbuffers.NewBuilder(0)

	return &StreamWriter{
		w: w,
		b: b,
	}
}

func (sw *StreamWriter) SetStructureLogging(flag bool) {
	if flag == true {
		sw.ss = NewStreamStructure()
	} else {
		sw.ss = nil
	}
}

func (sw *StreamWriter) Structure() *StreamStructure {
	if sw.ss == nil {
		log.Panicf("not collecting structure info")
	}

	return sw.ss
}

// pushStreamMilestone records a milestone pertaining to the stream.
func (sw *StreamWriter) pushStreamMilestone(milestoneType MilestoneType, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sw.ss != nil {
		sw.ss.Push(sw.position, milestoneType, StStream, "", comment)
	}

	return nil
}

// pushSeriesMilestone records a milestone of a constituent series. The UUID is
// optional as it will not be known until partway through the process.
func (sw *StreamWriter) pushSeriesMilestone(position int64, milestoneType MilestoneType, seriesUuid, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sw.ss != nil {
		if position == -1 {
			position = sw.position
		}

		sw.ss.Push(position, milestoneType, StSeries, seriesUuid, comment)
	}

	return nil
}

// pushStreamMilestone records a milestone pertaining to the stream.
func (sw *StreamWriter) pushMiscMilestone(milestoneType MilestoneType, comment string) (err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if sw.ss != nil {
		sw.ss.Push(sw.position, milestoneType, StMisc, "", comment)
	}

	return nil
}

func (sw *StreamWriter) bumpPosition(offset int64) {
	sw.position += offset
}

// writeShadowFooter writes a statically-sized footer that follows and describes
// a dynamically-sized footer.
func (sw *StreamWriter) writeShadowFooter(footerVersion uint16, footerType FooterType, footerLength uint16) (size int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	if footerType == FtStreamFooter {
		err := sw.pushStreamMilestone(MtShadowFooterHeadByte, "")
		log.PanicIf(err)
	} else if footerType == FtSeriesFooter {
		err := sw.pushSeriesMilestone(-1, MtShadowFooterHeadByte, "", "")
		log.PanicIf(err)
	}

	err = binary.Write(sw.w, binary.LittleEndian, footerVersion)
	log.PanicIf(err)

	size += 2

	err = binary.Write(sw.w, binary.LittleEndian, footerType)
	log.PanicIf(err)

	size += 1

	err = binary.Write(sw.w, binary.LittleEndian, footerLength)
	log.PanicIf(err)

	size += 2

	sw.bumpPosition(int64(size))

	if footerType == FtStreamFooter {
		err := sw.pushStreamMilestone(MtBoundaryMarker, "")
		log.PanicIf(err)
	} else if footerType == FtSeriesFooter {
		err := sw.pushSeriesMilestone(-1, MtBoundaryMarker, "", "")
		log.PanicIf(err)
	}

	_, err = sw.w.Write([]byte{0})
	log.PanicIf(err)

	size += 1

	sw.bumpPosition(1)

	streamWriterLogger.Debugf(nil, "writeShadowFooter: Wrote (%d) bytes for shadow footer.", size)

	// Keep us honest.
	if size != ShadowFooterSize {
		log.Panicf("shadow footer is not the right size")
	}

	return size, nil
}
