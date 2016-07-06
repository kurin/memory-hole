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

// Package afs provides an abstract file system, that can be used to write data
// to the backing store.
package afs

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/twinj/uuid"
)

type FileSystem struct {
	uuid string
	wdir string
	root *tdb
}

func Open(dir, u string) (*FileSystem, error) {
	if err := os.MkdirAll(filepath.Join(dir, u), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, u, "bolt.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	t := &tdb{db: db}
	if err := t.init(); err != nil {
		return nil, err
	}
	return &FileSystem{
		uuid: u,
		wdir: filepath.Join(dir, u),
		root: t,
	}, nil
}

func (fs *FileSystem) Open(name string) (*File, error) {
	u, err := fs.root.get(name)
	if err != nil {
		u = uuid.NewV4().String()
		if err := fs.root.add(name, u); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(filepath.Join(fs.wdir, u), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &File{
		File: f,
		name: name,
	}, nil
}

func (fs *FileSystem) Mkdir(name string) error {
	return fs.root.mkdir(name)
}

func (fs *FileSystem) Rmdir(name string) error {
	return fs.root.remove(name)
}

func (fs *FileSystem) Remove(name string) error {
	u, err := fs.root.get(name)
	if err != nil {
		return err
	}
	if err := fs.root.remove(name); err != nil {
		return err
	}
	return os.Remove(filepath.Join(fs.wdir, u))
}

func (fs *FileSystem) Stat(name string) (os.FileInfo, error) {
	u, err := fs.root.get(name)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(filepath.Join(fs.wdir, u))
	if err != nil {
		return nil, err
	}
	return &fileInfo{
		FileInfo: fi,
		name:     filepath.Base(name),
	}, nil
}

type Entry struct {
	Name      string
	Directory bool
}

func (fs *FileSystem) List(name string) ([]Entry, error) {
	ls, err := fs.root.list(name)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, e := range ls {
		out = append(out, Entry{Name: e.name, Directory: e.dir})
	}
	return out, nil
}

func (fs *FileSystem) Rename(from, to string) error {
	return fs.root.move(from, to)
}

func (fs *FileSystem) Finalize() (string, error) {
	return "", nil
}

func (fs *FileSystem) Destroy() error {
	fs.root.db.Close() // chuck error
	return os.RemoveAll(fs.wdir)
}
