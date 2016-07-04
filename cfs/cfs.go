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

// Package cfs provides the concrete (fuse) front-end to the abstract file
// system.
package cfs

import (
	"log"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/twinj/uuid"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FS struct {
	WorkDir    string
	Mountpoint string

	conn *fuse.Conn
	db   *bolt.DB
}

func (f *FS) Mount() error {
	c, err := fuse.Mount(f.Mountpoint)
	if err != nil {
		return err
	}
	f.conn = c

	db, err := bolt.Open(filepath.Join(f.WorkDir, "mh.db"), 0600, nil)
	if err != nil {
		c.Close()
		return err
	}
	f.db = db

	if err := fs.Serve(c, f); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}

func (f *FS) Close() error {
	if err := f.db.Close(); err != nil {
		f.conn.Close()
		return err
	}
	return f.conn.Close()
}

func (f *FS) Root() (fs.Node, error) {
	return &root{
		db: f.db,
	}, nil
}

type root struct {
	db *bolt.DB
}

func (r *root) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0700
	return nil
}

func (r *root) Lookup(ctx context.Context, name string) (fs.Node, error) {
	var a *archive
	if err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("archives"))
		if b == nil {
			return nil
		}
		sb := b.Bucket([]byte(name))
		if sb == nil {
			return nil
		}
		id := sb.Get([]byte("uuid"))
		if id == nil {
			return nil
		}
		u := uuid.New(id)
		a = &archive{
			uuid: u.String(),
		}
		return nil
	}); err != nil {
		log.Print(err)
		return nil, err
	}
	if a == nil {
		return nil, fuse.ENOENT
	}
	return a, nil
}

func (r *root) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	u := uuid.NewV4()
	if err := r.db.Update(func(tx *bolt.Tx) error {
		ab, err := tx.CreateBucketIfNotExists([]byte("archives"))
		if err != nil {
			return err
		}
		b, err := ab.CreateBucket([]byte(req.Name))
		if err != nil {
			return err
		}
		return b.Put([]byte("uuid"), u.Bytes())
	}); err != nil {
		return nil, err
	}
	return &archive{
		final: false,
		uuid:  u.String(),
	}, nil
}

func (r *root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var ent []fuse.Dirent
	if err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("archives"))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				ent = append(ent, fuse.Dirent{Name: string(k), Type: fuse.DT_Dir})
			}
			return nil
		})
	}); err != nil {
		log.Print(err)
		return nil, err
	}
	return ent, nil
}

func (r *root) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if !req.Dir {
		return fuse.ENOTSUP
	}
	if err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("archives"))
		if b == nil {
			return fuse.ENOENT
		}
		sb := b.Bucket([]byte(req.Name))
		if sb == nil {
			return fuse.ENOENT
		}
		return b.DeleteBucket([]byte(req.Name))
	}); err != nil {
		return err
	}
	return nil
}

type archive struct {
	final bool
	uuid  string
}

func (a *archive) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0700
	return nil
}

func (a *archive) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	out := []fuse.Dirent{
		{Name: "data", Type: fuse.DT_Dir},
		{Name: "status", Type: fuse.DT_File},
	}
	if !a.final {
		out = append(out, fuse.Dirent{Name: "done", Type: fuse.DT_File})
	}
	return out, nil
}

func (a *archive) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "done":
		if !a.final {
			return &done{}, nil
		}
	case "data":
		return &data{
			uuid: a.uuid,
		}, nil
	}
	return nil, fuse.ENOENT
}

type done struct{}

var donemsg = []byte("Remove this file to finalize this archive.\n")

func (done) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Size = uint64(len(donemsg))
	attr.Mode = 0644
	return nil
}

func (done) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	off := int(req.Offset)
	end := off + req.Size
	if end > len(donemsg) {
		end = len(donemsg)
	}
	resp.Data = donemsg[off:end]
	return nil
}

type data struct {
	uuid string
}

func (data) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0700
	return nil
}
