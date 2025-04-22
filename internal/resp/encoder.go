// Copyright (c) 2013 The github.com/go-redis/redis Authors.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:

// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package resp

import (
	"encoding"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/olric-data/olric/internal/util"
)

type encoder interface {
	io.Writer
	io.ByteWriter
	WriteString(s string) (n int, err error)
}

type Encoder struct {
	encoder

	lenBuf []byte
	numBuf []byte
}

func New(e encoder) *Encoder {
	return &Encoder{
		encoder: e,

		lenBuf: make([]byte, 64),
		numBuf: make([]byte, 64),
	}
}

func (e *Encoder) Encode(v interface{}) error {
	switch v := v.(type) {
	case nil:
		return e.string("")
	case string:
		return e.string(v)
	case []byte:
		return e.bytes(v)
	case int:
		return e.int(int64(v))
	case int8:
		return e.int(int64(v))
	case int16:
		return e.int(int64(v))
	case int32:
		return e.int(int64(v))
	case int64:
		return e.int(v)
	case uint:
		return e.uint(uint64(v))
	case uint8:
		return e.uint(uint64(v))
	case uint16:
		return e.uint(uint64(v))
	case uint32:
		return e.uint(uint64(v))
	case uint64:
		return e.uint(v)
	case float32:
		return e.float(float64(v))
	case float64:
		return e.float(v)
	case bool:
		if v {
			return e.int(1)
		}
		return e.int(0)
	case time.Time:
		e.numBuf = v.AppendFormat(e.numBuf[:0], time.RFC3339Nano)
		return e.bytes(e.numBuf)
	case time.Duration:
		return e.int(v.Nanoseconds())
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		return e.bytes(b)
	default:
		return fmt.Errorf(
			"olric: can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}

func (e *Encoder) bytes(b []byte) error {
	if _, err := e.Write(b); err != nil {
		return err
	}
	return nil
}

func (e *Encoder) string(s string) error {
	return e.bytes(util.StringToBytes(s))
}

func (e *Encoder) uint(n uint64) error {
	e.numBuf = strconv.AppendUint(e.numBuf[:0], n, 10)
	return e.bytes(e.numBuf)
}

func (e *Encoder) int(n int64) error {
	e.numBuf = strconv.AppendInt(e.numBuf[:0], n, 10)
	return e.bytes(e.numBuf)
}

func (e *Encoder) float(f float64) error {
	e.numBuf = strconv.AppendFloat(e.numBuf[:0], f, 'f', -1, 64)
	return e.bytes(e.numBuf)
}
