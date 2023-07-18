package keyv

import (
	"bytes"
	"encoding/binary"
)

type header struct {
	version byte
	klen    uint32
	vlen    uint32
}

const (
	MaxHeaderSize = 11
)

// encode serializes the data in to bytes. It uses the following layout
// +------------------------------------------
// | version | key_length | value_length
// +-----------------------------------------
func (h header) encode(out []byte) int {
	out[0] = h.version
	length := 1
	length += binary.PutUvarint(out[length:], uint64(h.klen))
	length += binary.PutUvarint(out[length:], uint64(h.vlen))

	return length
}

func (h *header) decode(in []byte) int {
	h.version = in[0]
	index := 1
	klen, len := binary.Uvarint(in[index:])
	h.klen = uint32(klen)
	index += len
	vlen, len := binary.Uvarint(in[index:])
	h.vlen = uint32(vlen)
	index += len
	return index
}

type KVEntry struct {
	header header
	key    []byte
	value  []byte
}

func NewKVEntry(key, value []byte) KVEntry {
	return KVEntry{
		header: header{
			version: byte(1),
			klen:    uint32(len(key)),
			vlen:    uint32(len(value)),
		},
		key:   key,
		value: value,
	}
}

// encode serializes the data in to bytes. It uses the following layout
// +------------------------------------------
// | header | key | value
// +-----------------------------------------
func (kv KVEntry) encode(buf *bytes.Buffer) int {
	h := make([]byte, MaxHeaderSize)
	hlen := kv.header.encode(h)
	len := Check2(buf.Write(h[:hlen]))
	len += Check2(buf.Write(kv.key))
	len += Check2(buf.Write(kv.value))
	return len
}

func (kv *KVEntry) decode(data []byte) int {
	hlen := kv.header.decode(data[0:MaxHeaderSize])
	index := hlen + int(kv.header.klen)
	kv.key = data[hlen:index]
	kv.value = data[index : index+int(kv.header.vlen)]
	return hlen + int(kv.header.klen) + int(kv.header.vlen)
}

type KVEntries []KVEntry

// encode serializes the data in to bytes. It uses the following layout
// +------------------------------------------
// | size | KVEntry [size | KVEntry ...]
// +-----------------------------------------
func (e KVEntries) encode(buf *bytes.Buffer) int {
	len := 0
	ibuf := new(bytes.Buffer)
	for _, b := range e {
		ilen := b.encode(ibuf)
		lb := make([]byte, 10)
		el := binary.PutUvarint(lb, uint64(ilen))
		len += Check2(buf.Write(lb[:el]))
		len += Check2(buf.Write(ibuf.Bytes()))
		ibuf.Reset()
	}
	return len
}

func (e *KVEntries) decode(data []byte) int {
	index := 0
	for {
		elen, ilen := binary.Uvarint(data[index:])
		index += ilen
		en := KVEntry{}

		en.decode(data[index : index+int(elen)])
		index += int(elen)

		*e = append(*e, en)
		if index >= len(data) {
			break
		}

	}

	return index
}

func (a KVEntries) Less(i, j int) bool {
	return bytes.Compare(a[i].key, a[j].key) != -1
}
func (a KVEntries) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a KVEntries) Len() int {
	return len(a)
}
