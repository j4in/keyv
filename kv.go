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
