package timetogo

import (
    "io"

    "github.com/dsoprea/go-logging"
)

// SeriesDataDatasourceWriter can be provided by the caller to write the series-
// data themselves if an `io.Reader` is too simple for them.
type SeriesDataDatasourceWriter interface {
    WriteData(w io.Writer, sf SeriesFooter) (n int, err error)
}

// SeriesDataDatasourceWriterWrapper wraps a simple `io.Reader` and satisfies
// the `SeriesDataDatasourceWriter` interface. It essentially converts a reader
// to a writer. This may not have a practical use, but we use it for testing.
type SeriesDataDatasourceWriterWrapper struct {
    r io.Reader
}

// WriteData copies the reader to the writer.
func (sddww SeriesDataDatasourceWriterWrapper) WriteData(w io.Writer, sf SeriesFooter) (n int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    count, err := io.Copy(w, sddww.r)
    log.PanicIf(err)

    return int(count), nil
}

// NewSeriesDataDatasourceWriterWrapperFromReader creates a new
// `SeriesDataDatasourceWriterWrapper` struct.
func NewSeriesDataDatasourceWriterWrapperFromReader(r io.Reader) SeriesDataDatasourceWriterWrapper {
    return SeriesDataDatasourceWriterWrapper{
        r: r,
    }
}

// SeriesDataDatasourceReader can be provided by a call to read the data
// themselves rather than providing an `io.Writer`.
type SeriesDataDatasourceReader interface {
    ReadData(r io.Reader, sf SeriesFooter) (n int, err error)
}

type SeriesDataDatasourceReaderWrapper struct {
    w io.Writer
}

// ReadData copies the reader to the writer.
func (sddww SeriesDataDatasourceReaderWrapper) ReadData(r io.Reader, sf SeriesFooter) (n int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    count, err := io.Copy(sddww.w, r)
    log.PanicIf(err)

    return int(count), nil
}

// NewSeriesDataDatasourceReaderWrapperFromWriter creates a new
// `SeriesDataDatasourceWriterWrapper` struct.
func NewSeriesDataDatasourceReaderWrapperFromWriter(w io.Writer) SeriesDataDatasourceReaderWrapper {
    return SeriesDataDatasourceReaderWrapper{
        w: w,
    }
}
