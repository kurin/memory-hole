package afs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/twinj/uuid"
)

func TestWriter(t *testing.T) {
	dir, err := ioutil.TempDir("", "afs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	table := []struct {
		files []string
		dir   string
		want  []Entry
	}{
		{
			files: []string{
				"/thing/1",
				"/thing/2",
			},
			dir: "/thing",
			want: []Entry{
				{
					Name: "1",
				},
				{
					Name: "2",
				},
			},
		},
		{
			files: []string{
				"/thing/1",
				"/thing/2",
				"/thing/3/ok",
			},
			dir: "/thing",
			want: []Entry{
				{
					Name: "1",
				},
				{
					Name: "2",
				},
				{
					Name:      "3",
					Directory: true,
				},
			},
		},
		{
			dir: "/",
		},
	}

	for _, e := range table {
		fs, err := Open(dir, uuid.NewV4().String())
		if err != nil {
			t.Error(err)
			continue
		}
		for _, f := range e.files {
			if err := writeFile(fs, f, "data\n"); err != nil {
				t.Error(err)
				continue
			}
		}
		ls, err := fs.List(e.dir)
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(ls, e.want) {
			t.Errorf("bad directory listing: got %#v want %#v", ls, e.want)
		}
	}
}

func writeFile(fs *FileSystem, name, body string) error {
	f, err := fs.Open(name)
	if err != nil {
		return err
	}
	buf := &bytes.Buffer{}
	buf.WriteString(body)
	if _, err := io.Copy(f, buf); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
