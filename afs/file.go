package afs

import (
	"os"
	"path/filepath"
)

type File struct {
	*os.File
	name string
}

func (f *File) Stat() (os.FileInfo, error) {
	s, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return &fileInfo{
		FileInfo: s,
		name:     filepath.Base(f.name),
	}, nil
}

type fileInfo struct {
	os.FileInfo
	name string
}

func (fi *fileInfo) Name() string {
	return fi.name
}
