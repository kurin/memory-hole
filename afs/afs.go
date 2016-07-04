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

	"github.com/twinj/uuid"
)

type FileSystem struct {
	uuid string
	wdir string
	root *node
}

func New(dir string) (*FileSystem, error) {
	u := uuid.NewV4().String()
	if err := os.MkdirAll(filepath.Join(dir, u), 0700); err != nil {
		return nil, err
	}
	return &FileSystem{
		uuid: u,
		wdir: filepath.Join(dir, u),
		root: &node{dir: true},
	}, nil
}

func (fs *FileSystem) Open(name string) (*File, error) {
	var create bool
	n, err := fs.root.get(name)
	if err != nil {
		add, err := fs.root.add(name)
		if err != nil {
			return nil, err
		}
		create = true
		add.uuid = uuid.NewV4().String()
		n = add
	}
	u := n.uuid
	var f *os.File
	if create {
		f, err = os.Create(filepath.Join(fs.wdir, u))
	} else {
		f, err = os.Open(filepath.Join(fs.wdir, u))
	}
	if err != nil {
		return nil, err
	}
	return &File{
		File: f,
		name: name,
	}, nil
}

func (fs *FileSystem) Remove(name string) error {
	n, err := fs.root.remove(name, true)
	if err != nil {
		return err
	}
	return os.Remove(filepath.Join(fs.wdir, n.uuid))
}

func (fs *FileSystem) Finalize() (string, error) {
	return "", nil
}
