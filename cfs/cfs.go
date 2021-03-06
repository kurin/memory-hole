// Package cfs provides the concrete (fuse) front-end to the abstract file
// system.
package cfs

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/kurin/memory-hole/afs"
	"github.com/twinj/uuid"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FS struct {
	WorkDir    string
	Mountpoint string

	mux  sync.RWMutex
	fss  map[string]*afs.FileSystem
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
	var nilmap bool
	f.mux.RLock()
	if f.fss == nil {
		nilmap = true
	}
	f.mux.RUnlock()
	if nilmap {
		f.mux.Lock()
		if f.fss == nil {
			f.fss = make(map[string]*afs.FileSystem)
		}
		f.mux.Unlock()
	}
	return &root{
		wdir: f.WorkDir,
		db:   f.db,
		mux:  &f.mux,
		fss:  f.fss,
	}, nil
}

type root struct {
	wdir string
	db   *bolt.DB
	mux  *sync.RWMutex
	fss  map[string]*afs.FileSystem
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
		r.mux.Lock()
		fs, ok := r.fss[u.String()]
		if !ok {
			nfs, err := afs.Open(r.wdir, u.String())
			if err != nil {
				return err
			}
			r.fss[u.String()] = nfs
			fs = nfs
		}
		r.mux.Unlock()
		a = &archive{
			uuid: u.String(),
			fs:   fs,
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
	var fs *afs.FileSystem
	if err := r.db.Update(func(tx *bolt.Tx) error {
		ab, err := tx.CreateBucketIfNotExists([]byte("archives"))
		if err != nil {
			return err
		}
		b, err := ab.CreateBucket([]byte(req.Name))
		if err != nil {
			return err
		}
		// Do this in the DB transaction so that any errors roll back the DB.
		nfs, err := afs.Open(r.wdir, u.String())
		fs = nfs
		if err != nil {
			return err
		}
		r.mux.Lock()
		r.fss[u.String()] = fs
		r.mux.Unlock()
		return b.Put([]byte("uuid"), u.Bytes())
	}); err != nil {
		return nil, err
	}
	return &archive{
		final: false,
		uuid:  u.String(),
		fs:    fs,
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
		u := uuid.New(sb.Get([]byte("uuid")))
		r.mux.Lock()
		if fs, ok := r.fss[u.String()]; ok {
			if err := fs.Destroy(); err != nil {
				return err
			}
		}
		r.mux.Unlock()
		return b.DeleteBucket([]byte(req.Name))
	}); err != nil {
		return err
	}
	return nil
}

type archive struct {
	final bool
	uuid  string
	fs    *afs.FileSystem
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
			path: "/",
			fs:   a.fs,
		}, nil
	}
	return nil, fuse.ENOENT
}

type done struct{}

var donemsg = []byte("Remove this file to finalize the archive.\n")

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
	path string
	fs   *afs.FileSystem
}

func (d *data) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Valid = time.Nanosecond
	attr.Mode = os.ModeDir | 0700
	return nil
}

func (d *data) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var out []fuse.Dirent
	ls, err := d.fs.List(d.path)
	if err != nil {
		return nil, err
	}
	for _, e := range ls {
		t := fuse.DT_File
		if e.Directory {
			t = fuse.DT_Dir
		}
		out = append(out, fuse.Dirent{Name: e.Name, Type: t})
	}
	return out, nil
}

func (d *data) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	name := req.Name
	ls, err := d.fs.List(d.path)
	if err != nil {
		return nil, err
	}
	var found bool
	var dir bool
	for _, e := range ls {
		if e.Name == name {
			found = true
			dir = e.Directory
			break
		}
	} // TODO: easy speedups here
	if !found {
		return nil, fuse.ENOENT
	}
	var node fs.Node
	if dir {
		node = &data{
			uuid: d.uuid,
			path: filepath.Join(d.path, name),
			fs:   d.fs,
		}
	} else {
		node = &file{
			fs:   d.fs,
			path: filepath.Join(d.path, name),
		}
	}
	if err := node.Attr(ctx, &resp.Attr); err != nil {
		return nil, err
	}
	resp.EntryValid = 1
	return node, nil
}

func (d *data) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if err := d.fs.Mkdir(filepath.Join(d.path, req.Name)); err != nil {
		return nil, err
	}
	return &data{
		uuid: d.uuid,
		path: filepath.Join(d.path, req.Name),
		fs:   d.fs,
	}, nil
}

func (d *data) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return d.fs.Rmdir(filepath.Join(d.path, req.Name))
	}
	return d.fs.Remove(filepath.Join(d.path, req.Name))
}

func (d *data) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f, err := d.fs.Open(filepath.Join(d.path, req.Name))
	if err != nil {
		return nil, nil, err
	}
	r := &file{
		f:    f,
		fs:   d.fs,
		path: filepath.Join(d.path, req.Name),
	}
	return r, r, nil
}

func (d *data) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd, ok := newDir.(*data)
	if !ok {
		return fmt.Errorf("can't move files out of the data dir")
	}
	return d.fs.Rename(filepath.Join(d.path, req.OldName), filepath.Join(nd.path, req.NewName))
}

type file struct {
	path string
	f    *afs.File
	fs   *afs.FileSystem
}

func (f *file) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := f.fs.Stat(f.path)
	if err != nil {
		return err
	}
	attr.Size = uint64(fi.Size())
	attr.Mode = 0600
	return nil
}

func (f *file) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	h, err := f.fs.Open(f.path)
	if err != nil {
		return nil, err
	}
	f.f = h
	return f, nil
}

func (f *file) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if f.f != nil {
		mf := f.f
		f.f = nil
		return mf.Close()
	}
	return nil
}

func (f *file) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := f.f.WriteAt(req.Data, req.Offset)
	resp.Size = n
	return err
}

func (f *file) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	b := make([]byte, req.Size)
	n, err := f.f.ReadAt(b, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = b[:n]
	return nil
}

func (f *file) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return f.f.Sync()
}
