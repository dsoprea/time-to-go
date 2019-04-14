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
	w io.Writer
	b *flatbuffers.Builder
}

func NewStreamWriter(w io.Writer) *StreamWriter {
	b := flatbuffers.NewBuilder(0)

	return &StreamWriter{
		w: w,
		b: b,
	}
}

// writeShadowFooter writes a statically-sized footer that follows and describes
// a dynamically-sized footer.
func (sw *StreamWriter) writeShadowFooter(footerVersion uint16, footerType FooterType, footerLength uint16) (size int, err error) {
	defer func() {
		if state := recover(); state != nil {
			err = log.Wrap(state.(error))
		}
	}()

	err = binary.Write(sw.w, binary.LittleEndian, footerVersion)
	log.PanicIf(err)

	size += 2

	err = binary.Write(sw.w, binary.LittleEndian, footerType)
	log.PanicIf(err)

	size += 1

	err = binary.Write(sw.w, binary.LittleEndian, footerLength)
	log.PanicIf(err)

	size += 2

	_, err = sw.w.Write([]byte{0})
	log.PanicIf(err)

	size += 1

	streamWriterLogger.Debugf(nil, "writeShadowFooter: Wrote (%d) bytes for shadow footer.", size)

	// Keep us honest.
	if size != ShadowFooterSize {
		log.Panicf("shadow footer is not the right size")
	}

	return size, nil
}
