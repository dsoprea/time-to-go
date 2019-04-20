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

func (rcv *StreamIndexedSequenceInfo) Uuid() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *StreamIndexedSequenceInfo) HeadRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateHeadRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *StreamIndexedSequenceInfo) TailRecordEpoch() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateTailRecordEpoch(n uint64) bool {
	return rcv._tab.MutateUint64Slot(8, n)
}

func (rcv *StreamIndexedSequenceInfo) AbsolutePosition() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StreamIndexedSequenceInfo) MutateAbsolutePosition(n int64) bool {
	return rcv._tab.MutateInt64Slot(10, n)
}

func StreamIndexedSequenceInfoStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func StreamIndexedSequenceInfoAddUuid(builder *flatbuffers.Builder, uuid flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(uuid), 0)
}
func StreamIndexedSequenceInfoAddHeadRecordEpoch(builder *flatbuffers.Builder, headRecordEpoch uint64) {
	builder.PrependUint64Slot(1, headRecordEpoch, 0)
}
func StreamIndexedSequenceInfoAddTailRecordEpoch(builder *flatbuffers.Builder, tailRecordEpoch uint64) {
	builder.PrependUint64Slot(2, tailRecordEpoch, 0)
}
func StreamIndexedSequenceInfoAddAbsolutePosition(builder *flatbuffers.Builder, absolutePosition int64) {
	builder.PrependInt64Slot(3, absolutePosition, 0)
}
func StreamIndexedSequenceInfoEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
