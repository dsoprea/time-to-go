package timetogo

import (
    "io"

    "encoding/gob"

    "github.com/dsoprea/go-logging"
)

// >>>>>>>>>>>>>>>>>>>>>
// Datasource interfaces
// <<<<<<<<<<<<<<<<<<<<<

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

// SeriesDataDatasourceReaderWrapper wraps a simple `io.Writer` and satisfies
// the `SeriesDataDatasourceReader` interface. It essentially converts a writer
// to a reader. This may not have a practical use, but we use it for testing.
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

// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
// gob encoder/decoder single-object wrapper
// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

// GobSingleObjectDecoderDatasource wraps a `gob.Decoder` as a `SeriesDataDatasourceReader`.
type GobSingleObjectDecoderDatasource struct {
    outputValue interface{}
}

// NewGobSingleObjectDecoderDatasource returns a new `GobSingleObjectDecoderDatasource` struct.
func NewGobSingleObjectDecoderDatasource(outputValue interface{}) *GobSingleObjectDecoderDatasource {
    return &GobSingleObjectDecoderDatasource{
        outputValue: outputValue,
    }
}

// ReadData is called when series data needs to be read and decodes the raw
// series-data into the struct that we were initialized with.
func (gdd *GobSingleObjectDecoderDatasource) ReadData(r io.Reader, sf SeriesFooter) (n int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    d := gob.NewDecoder(r)

    err = d.Decode(gdd.outputValue)
    log.PanicIf(err)

    return -1, nil
}

// GobSingleObjectEncoderDatasource wraps a `gob.Encoder` as a `SeriesDataDatasourceWriter`.
type GobSingleObjectEncoderDatasource struct {
    inputValue interface{}
}

// NewGobSingleObjectEncoderDatasource returns a new GobSingleObjectEncoderDatasource struct.
func NewGobSingleObjectEncoderDatasource(inputValue interface{}) *GobSingleObjectEncoderDatasource {
    return &GobSingleObjectEncoderDatasource{
        inputValue: inputValue,
    }
}

// WriteData is called when series data needs to be written and encodes the
// struct that we were initialized with into the writer we are given.
func (ged GobSingleObjectEncoderDatasource) WriteData(w io.Writer, sf SeriesFooter) (n int, err error) {
    defer func() {
        if state := recover(); state != nil {
            err = log.Wrap(state.(error))
        }
    }()

    e := gob.NewEncoder(w)

    err = e.Encode(ged.inputValue)
    log.PanicIf(err)

    return n, nil
}
