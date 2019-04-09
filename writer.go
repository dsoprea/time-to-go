package timetogo

import (
    "io"

    "github.com/dsoprea/go-logging"
)

type CountingWriter struct {
    w        io.Writer
    position int
}

func NewCountingWriter(w io.Writer) *CountingWriter {
    return &CountingWriter{
        w: w,
    }
}

func (cw *CountingWriter) Write(data []byte) (n int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    n, err = cw.w.Write(data)
    log.PanicIf(err)

    cw.position += n

    return n, nil
}

func (cw *CountingWriter) Position() int {
    return cw.position
}
