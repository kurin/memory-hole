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
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FS struct {
	WorkDir    string
	Mountpoint string

	conn *fuse.Conn
}

func (f *FS) Mount() error {
	c, err := fuse.Mount(f.Mountpoint)
	if err != nil {
		return err
	}
	f.conn = c

	if err := fs.Serve(c, f); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}

func (f *FS) Close() error {
	return f.conn.Close()
}

func (f *FS) Root() (fs.Node, error) {
	return f, nil
}

func (f *FS) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = 2
	attr.Mode = os.ModeDir | 0700
	return nil
}
