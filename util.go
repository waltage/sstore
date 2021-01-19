package sstore

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/waltage/sstore/pb"
)

func ef(frmt string, msg ...interface{}) error {
	err := fmt.Errorf(frmt, msg...)
	return err
}

func Gobber(i interface{}) []byte {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	_ = e.Encode(i)
	return buf.Bytes()
}

func DeGobber(in []byte, target interface{}) error {
	buf := bytes.NewBuffer(in)
	d := gob.NewDecoder(buf)
	return d.Decode(target)
}

func pbKeytoKey(key *pb.Key) *Key {
	return &Key{
		Bucket:  key.Bucket.Name,
		ID:      key.Id,
		Version: key.Version,
	}
}

func keyToPBKey(in *Key) *pb.Key {
	return &pb.Key{
		Bucket:  &pb.Bucket{Name: in.Bucket},
		Id:      in.ID,
		Version: in.Version,
	}
}

type pbStorable struct {
	bytes []byte
}

func (pbs *pbStorable) Encode() []byte {
	return pbs.bytes
}

func (pbs *pbStorable) Decode(in []byte) error {
	pbs.bytes = in
	return nil
}
