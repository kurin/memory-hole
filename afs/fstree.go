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
	if path == "" {
		return nil
	}
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
			if parts[i] == "" {
				continue
			}
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
			if parts[i] == "" {
				continue
			}
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

func (t *tdb) move(from, to string) error {
	fparent := filepath.Dir(from)
	fname := filepath.Base(from)
	tparent := filepath.Dir(to)
	tname := filepath.Base(to)
	return t.db.Update(func(tx *bolt.Tx) error {
		fb, err := getNode(fparent, tx)
		if err != nil {
			return err
		}
		tb, err := getNode(tparent, tx)
		if err != nil {
			return err
		}
		fba := fb.Bucket([]byte(fname))
		if fba == nil {
			return fmt.Errorf("%s: not found", from)
		}
		tba, err := tb.CreateBucket([]byte(tname))
		if err != nil {
			return err
		}
		if err := copyBucket(tba, fba); err != nil {
			return err
		}
		return fb.DeleteBucket([]byte(fname))
	})
}

func copyBucket(dst, src *bolt.Bucket) error {
	return src.ForEach(func(k, v []byte) error {
		if v == nil {
			nb, err := dst.CreateBucket(k)
			if err != nil {
				return err
			}
			b := src.Bucket(k)
			if err := copyBucket(nb, b); err != nil {
				return err
			}
			return nil
		}
		return dst.Put(k, v)
	})
}

func getNode(name string, tx *bolt.Tx) (*bolt.Bucket, error) {
	path := strings.Trim(name, "/")
	parts := strings.Split(path, "/")

	b := tx.Bucket([]byte("/"))
	if b == nil {
		return nil, fmt.Errorf("/: not found")
	}
	for i := 0; i < len(parts); i++ {
		piece := parts[i]
		if piece == "" {
			continue
		}
		nb := b.Bucket([]byte(piece))
		if nb == nil {
			return nil, fmt.Errorf("%s: can't find %s", name, piece)
		}
		t := nb.Get([]byte("type"))
		if len(t) == 0 {
			return nil, fmt.Errorf("%s: untyped node", piece)
		}
		if t[0] != dirType {
			return nil, fmt.Errorf("%s: not a directory")
		}
		b = nb
	}
	return b, nil
}
