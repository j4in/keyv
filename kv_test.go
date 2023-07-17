package keyv

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func Test_header_encode(t *testing.T) {
	in := header{
		version: byte(255),
		klen:    math.MaxUint32,
		vlen:    math.MaxUint32,
	}

	data := make([]byte, MaxHeaderSize)
	enclen := in.encode(data)

	out := new(header)
	declen := out.decode(data)
	if enclen != declen {
		t.Errorf("Expected length to be the same, got enc = %d and dec %d", enclen, declen)
	}

	if !reflect.DeepEqual(&in, out) {
		t.Errorf("Mismatch in data. \n\t in : %+v \n\t out: %+v ", &in, out)
	}

}

func TestKVEntry(t *testing.T) {
	i := 100
	key := sb(fmt.Sprintf("key:%d", i))
	value := sb(fmt.Sprintf("val:%d", i))
	entry := KVEntry{
		header: header{
			version: byte(1),
			klen:    uint32(len(key)),
			vlen:    uint32(len(value)),
		},
		key:   key,
		value: value,
	}

	buf := new(bytes.Buffer)
	enclen := entry.encode(buf)
	out := new(KVEntry)
	declen := out.decode(buf.Bytes())

	if enclen != declen {
		t.Error("Mismatch in length")
	}

	if !reflect.DeepEqual(&entry, out) {
		t.Errorf("Expected successful serde \n\t in : %+v \n\t out: %+v", &entry, out)
	}

}

func sb(s string) []byte {
	return []byte(s)
}
