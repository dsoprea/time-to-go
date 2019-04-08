// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package ttgstream

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type SeriesFooter1 struct {
	_tab flatbuffers.Table
}

func GetRootAsSeriesFooter1(buf []byte, offset flatbuffers.UOffsetT) *SeriesFooter1 {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SeriesFooter1{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *SeriesFooter1) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SeriesFooter1) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *SeriesFooter1) HeadRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SeriesFooter1) MutateHeadRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *SeriesFooter1) TailRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SeriesFooter1) MutateTailRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *SeriesFooter1) BytesLength() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SeriesFooter1) MutateBytesLength(n uint64) bool {
	return rcv._tab.MutateUint64Slot(8, n)
}

func (rcv *SeriesFooter1) RecordCount() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SeriesFooter1) MutateRecordCount(n uint64) bool {
	return rcv._tab.MutateUint64Slot(10, n)
}

func (rcv *SeriesFooter1) OriginalFilename() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *SeriesFooter1) SourceSha1() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *SeriesFooter1) DataFnv1aChecksum() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *SeriesFooter1) MutateDataFnv1aChecksum(n uint32) bool {
	return rcv._tab.MutateUint32Slot(16, n)
}

func SeriesFooter1Start(builder *flatbuffers.Builder) {
	builder.StartObject(7)
}
func SeriesFooter1AddHeadRecordEpoch(builder *flatbuffers.Builder, headRecordEpoch uint64) {
	builder.PrependUint64Slot(0, headRecordEpoch, 0)
}
func SeriesFooter1AddTailRecordEpoch(builder *flatbuffers.Builder, tailRecordEpoch uint64) {
	builder.PrependUint64Slot(1, tailRecordEpoch, 0)
}
func SeriesFooter1AddBytesLength(builder *flatbuffers.Builder, bytesLength uint64) {
	builder.PrependUint64Slot(2, bytesLength, 0)
}
func SeriesFooter1AddRecordCount(builder *flatbuffers.Builder, recordCount uint64) {
	builder.PrependUint64Slot(3, recordCount, 0)
}
func SeriesFooter1AddOriginalFilename(builder *flatbuffers.Builder, originalFilename flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(originalFilename), 0)
}
func SeriesFooter1AddSourceSha1(builder *flatbuffers.Builder, sourceSha1 flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(sourceSha1), 0)
}
func SeriesFooter1AddDataFnv1aChecksum(builder *flatbuffers.Builder, dataFnv1aChecksum uint32) {
	builder.PrependUint32Slot(6, dataFnv1aChecksum, 0)
}
func SeriesFooter1End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}