// Copyright 2016 Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/twinj/uuid"
)

type tdb struct {
	db *bolt.DB
}

const (
	dirType  byte = 0x01
	fileType      = 0x02
)

func (t *tdb) init() error {
	return t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("/"))
		if b != nil {
			return nil
		}
		nb, err := tx.CreateBucket([]byte("/"))
		if err != nil {
			return err
		}
		return nb.Put([]byte("type"), []byte{dirType})
	})
}

func (t *tdb) mkdir(name string) error {
	path := strings.Trim(name, "/")
	parts := strings.Split(path, "/")
	return t.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("/"))
		if err != nil {
			return err
		}
		for i := 0; i < len(parts); i++ {
			piece := parts[i]
			sb, err := b.CreateBucketIfNotExists([]byte(piece))
			if err != nil {
				return err
			}
			b = sb
			bt := b.Get([]byte("type"))
			if len(bt) == 0 {
				if err := b.Put([]byte("type"), []byte{dirType}); err != nil {
					return err
				}
			} else if bt[0] != dirType {
				return fmt.Errorf("%s: not a directory", piece)
			}
		}
		return nil
	})
}

type tent struct { // "tree entry"
	name string
	dir  bool
}

func (t *tdb) list(name string) ([]tent, error) {
	path := strings.Trim(name, "/")
	parts := strings.Split(path, "/")
	var out []tent
	if err := t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("/"))
		if b == nil {
			return fmt.Errorf("%s: can't access /", name)
		}
		for i := 0; i < len(parts); i++ {
			if parts[i] == "" {
				continue
			}
			b = b.Bucket([]byte(parts[i]))
			if b == nil {
				return fmt.Errorf("%s: can't access %s", name, parts[i])
			}
		}
		bt := b.Get([]byte("type"))
		if len(bt) == 0 || bt[0] != dirType {
			return fmt.Errorf("%s: not a directory", name)
		}
		return b.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			sb := b.Bucket(k)
			if sb == nil {
				return nil // ???
			}
			sbt := sb.Get([]byte("type"))
			if len(sbt) == 0 {
				return fmt.Errorf("%s/%s: type-less entry", name, string(k))
			}
			out = append(out, tent{
				dir:  sbt[0] == dirType,
				name: string(k),
			})
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (t *tdb) add(name, id string) error {
	path := filepath.Dir(name)
	base := filepath.Base(name)

	if err := t.mkdir(path); err != nil {
		return err
	}

	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")

	return t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("/"))
		if b == nil {
			return fmt.Errorf("%s: can't access /", name)
		}
		for i := 0; i < len(parts); i++ {
			if parts[i] == "" {
				continue
			}
			b = b.Bucket([]byte(parts[i]))
			if b == nil {
				return fmt.Errorf("%s: can't access %s", name, parts[i])
			}
		}
		b, err := b.CreateBucket([]byte(base))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("type"), []byte{fileType}); err != nil {
			return err
		}
		u, err := uuid.Parse(id)
		if err != nil {
			return err
		}
		return b.Put([]byte("uuid"), u.Bytes())
	})
}

func (t *tdb) get(name string) (string, error) {
	path := filepath.Dir(name)
	base := filepath.Base(name)

	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")

	var u string
	if err := t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("/"))
		if b == nil {
			return fmt.Errorf("%s: can't access /", name)
		}
		for i := 0; i < len(parts); i++ {
			b = b.Bucket([]byte(parts[i]))
			if b == nil {
				return fmt.Errorf("%s: can't access %s", name, parts[i])
			}
		}
		b = b.Bucket([]byte(base))
		if b == nil {
			return fmt.Errorf("%s: no such file or directory", name)
		}
		bt := b.Get([]byte("type"))
		if len(bt) == 0 || bt[0] != fileType {
			return fmt.Errorf("%s: not a file", name)
		}
		u = uuid.New(b.Get([]byte("uuid"))).String()
		return nil
	}); err != nil {
		return "", err
	}
	return u, nil
}

func (t *tdb) remove(name string) error {
	path := filepath.Dir(name)
	base := filepath.Base(name)

	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")

	return t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("/"))
		if b == nil {
			return fmt.Errorf("%s: can't access /", name)
		}
		for i := 0; i < len(parts); i++ {
			b = b.Bucket([]byte(parts[i]))
			if b == nil {
				return fmt.Errorf("%s: can't access %s", name, parts[i])
			}
		}
		p := b
		b = b.Bucket([]byte(base))
		if b == nil {
			return fmt.Errorf("%s: no such file or directory", name)
		}
		var c int
		b.ForEach(func(k, v []byte) error {
			if v == nil {
				c++
			}
			return nil
		})
		if c > 0 {
			return fmt.Errorf("%s: directory not empty", name)
		}
		return p.DeleteBucket([]byte(base))
	})
}
