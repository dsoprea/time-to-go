[![GoDoc](https://godoc.org/github.com/dsoprea/time-to-go?status.svg)](https://godoc.org/github.com/dsoprea/time-to-go)
[![Build Status](https://travis-ci.org/dsoprea/time-to-go.svg?branch=master)](https://travis-ci.org/dsoprea/time-to-go)
[![Coverage Status](https://coveralls.io/repos/github/dsoprea/time-to-go/badge.svg?branch=master)](https://coveralls.io/github/dsoprea/time-to-go?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/dsoprea/time-to-go)](https://goreportcard.com/report/github.com/dsoprea/time-to-go)

# Overview

Allows efficient persistence of multiple series of time-series data, efficient updates, and efficient retrieval.


# Use Case

This project was primarily designed as an optimization when you have many data files that are expensively loaded into a time-series memory structure and would like to persist that data between invocations. To persist, that single time-series can be cut into several well-defined ranges of time (such as months or years), encoded (e.g. flatbuffers or gob) and stored to disk with `time-to-go` and then, on future invocations, loaded one series at a time, on demand. It should (and does) also accomodate updates to individual time-series if any source-data changes, without disrupting the other series, while also keeping all of this data in a single stream rather than a whole filesystem tree.

The time-series data itself is an arbitrary blob provided via a `Reader` given by the caller, along with head and tail timestamps, filename, record count, and data length.


# Stream Structure

Each series is followed by a series footer, which is followed by a brief "shadow" footer describing a version and length, and each stream ends with a stream footer, followed by another shadow footer. The stream is read from back to front, and summary information about all series are stored in the stream footer. So, it is very quick to determine which series will contain a certain timestamp and where those series are in the stream. This backwards-to-forwards methodology is meant to optimize updates.


# Update Complexity

If an update is performed but none of the time-series stored at the front of the stream have been changed, no writes for those series are performed. If we're only updating existing series and they're in the same order as in the stream, only the stream footer is updated. If one or more series are dropped from the stream, then any following series that are to be kept will be copied directly from that later position in the stream to the earlier position. In all of these cases, the caller is not required to provide the series data, and the caller will know in advance whether or not they need to provide that data by which series it is passing for the update.


# Notes

- An update on a stream may produce a smaller stream, but will be dealing with byte streams and will not be able to truncate the stream to a shorter size. There is a `Truncater` interface defined that matches the [Truncate()](https://golang.org/pkg/os/#File.Truncate) method on `File`. If the `ReadWriteSeeker` struct that is passed into `Updater` also satisfies the `Truncater` interface, it will automatically be truncated. Otherwise, it will be the caller's responsibility to truncate that stream (whether it's a byte slice, physical file, etc...) to the right length using the length returned by the update.
