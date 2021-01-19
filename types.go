package sstore

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	bSep = "::"
	vSep = ".v"
)

var (
	keyFormat     = fmt.Sprintf("%%s%s%%s%s%%d", bSep, vSep)
	keyByteFormat = fmt.Sprintf("%%s%s%%d", vSep)
	keyReg        = fmt.Sprintf("(.*)%s(.+)%s([0-9]*)", bSep, vSep)
	boltReg       = fmt.Sprintf("(.+)%s([0-9]*)", vSep)
)

type Storable interface {
	Encode() []byte
	Decode([]byte) error
}

type Key struct {
	Bucket  string
	ID      string
	Version uint64
}

func (k *Key) String() string {
	return fmt.Sprintf(keyFormat, k.Bucket, k.ID, k.Version)
}

func (k *Key) Bytes() (bucket []byte, key []byte) {
	bucket = k.BucketBytes()
	key = k.KeyBytes()
	return
}

func (k *Key) Copy() *Key {
	newKey := Key{
		Bucket:  k.Bucket,
		ID:      k.ID,
		Version: k.Version,
	}
	return &newKey
}

func (k *Key) IsAfter(key *Key) bool {
	if k.Bucket != key.Bucket {
		return k.Bucket > key.ID
	}
	if k.ID != key.ID {
		return k.ID > key.ID
	}
	return k.Version > key.Version
}

func (k *Key) IsVersioned() bool {
	return k.Version != 0
}

func (k *Key) BucketBytes() []byte {
	return []byte(k.Bucket)
}

func (k *Key) KeyBytes() []byte {
	return []byte(fmt.Sprintf(keyByteFormat, k.ID, k.Version))
}

func ParseKeyString(key string) *Key {
	nKey := Key{}
	part1 := strings.Split(key, bSep)
	next := key
	if len(part1) == 2 {
		nKey.Bucket = part1[0]
		next = part1[1]
	}
	part2 := strings.Split(next, vSep)
	if len(part2) == 2 {
		nKey.ID = part2[0]
		nKey.Version, _ = strconv.ParseUint(part2[1], 10, 64)
	} else {
		nKey.ID = next
	}

	return &nKey

}

func idAndVersionFromBoltKey(in []byte) (string, uint64, error) {
	_id, err := idFromBoltKey(in)
	if err != nil {
		return "", 0, err
	}
	_vz, err := vzFromBoltKey(in)
	if err != nil {
		return "", 0, err
	}

	return _id, _vz, nil
}

func idFromBoltKey(in []byte) (string, error) {
	match := regexp.MustCompile(boltReg)
	rez := match.FindAllStringSubmatch(string(in), -1)
	if rez == nil {
		dbLog.Warn("could not parse key '%s'", string(in))
		return "", ef("malformed key")
	}
	itm := rez[0]
	return itm[1], nil
}

func vzFromBoltKey(in []byte) (uint64, error) {
	match := regexp.MustCompile(boltReg)
	rez := match.FindAllStringSubmatch(string(in), -1)
	if rez == nil {
		dbLog.Warn("could not parse key '%s'", string(in))
		return 0, ef("malformed key")
	}
	itm := rez[0]
	vz, err := strconv.ParseUint(itm[2], 10, 64)
	if err != nil {
		dbLog.Warn("conversion error:", itm[2])
		return 0, err
	}
	return vz, nil
}

func SortKeys(in []*Key) {
	sort.SliceStable(in, func(i int, j int) bool {
		return in[j].IsAfter(in[i])
	})
}
