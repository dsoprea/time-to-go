// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package ttgstream

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type StreamIndexedSequenceInfo struct {
	_tab flatbuffers.Table
}

func GetRootAsStreamIndexedSequenceInfo(buf []byte, offset flatbuffers.UOffsetT) *StreamIndexedSequenceInfo {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &StreamIndexedSequenceInfo{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *StreamIndexedSequenceInfo) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *StreamIndexedSequenceInfo) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *StreamIndexedSequenceInfo) HeadRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateHeadRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *StreamIndexedSequenceInfo) TailRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateTailRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *StreamIndexedSequenceInfo) OriginalFilename() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *StreamIndexedSequenceInfo) AbsolutePosition() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateAbsolutePosition(n uint64) bool {
	return rcv._tab.MutateUint64Slot(10, n)
}

func StreamIndexedSequenceInfoStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func StreamIndexedSequenceInfoAddHeadRecordEpoch(builder *flatbuffers.Builder, headRecordEpoch uint64) {
	builder.PrependUint64Slot(0, headRecordEpoch, 0)
}
func StreamIndexedSequenceInfoAddTailRecordEpoch(builder *flatbuffers.Builder, tailRecordEpoch uint64) {
	builder.PrependUint64Slot(1, tailRecordEpoch, 0)
}
func StreamIndexedSequenceInfoAddOriginalFilename(builder *flatbuffers.Builder, originalFilename flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(originalFilename), 0)
}
func StreamIndexedSequenceInfoAddAbsolutePosition(builder *flatbuffers.Builder, absolutePosition uint64) {
	builder.PrependUint64Slot(3, absolutePosition, 0)
}
func StreamIndexedSequenceInfoEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}