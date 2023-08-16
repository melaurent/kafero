// Copyright © 2014 Steve Francia <spf@spf13.com>.
// Copyright 2009 The Go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"bytes"
	"fmt"
	"github.com/melaurent/kafero"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

var testName = "test.txt"

// var gcsFs, _ = NewTestGcsFs()

var testRegistry map[kafero.Fs][]string = make(map[kafero.Fs][]string)

func GetTmpDir(fs kafero.Fs) string {
	name, err := kafero.TempDir(fs, "", "afero")
	if err != nil {
		panic(fmt.Sprint("unable to work with test dir", err))
	}
	testRegistry[fs] = append(testRegistry[fs], name)

	return name
}

func GetTmpFile(fs kafero.Fs) kafero.File {
	x, err := kafero.TempFile(fs, "", "afero")

	if err != nil {
		panic(fmt.Sprint("unable to work with temp file: ", err))
	}

	testRegistry[fs] = append(testRegistry[fs], x.Name())

	return x
}

// TODO test "exotic" flags behavior with all the different file systems

// Read with length 0 should not return EOF.
func TestRead0(t *testing.T, fs kafero.Fs) {
	f := GetTmpFile(fs)
	defer f.Close()
	_, err := f.WriteString("Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")
	if err != nil {
		t.Errorf("%v: WriteString failed: %v", fs.Name(), err)
		return
	}

	var b []byte
	// b := make([]byte, 0)
	n, err := f.Read(b)
	if n != 0 || err != nil {
		t.Errorf("%v: Read(0) = %d, %v, want 0, nil", fs.Name(), n, err)
		return
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		t.Errorf("%v: Seek(0, 0) failed: %v", fs.Name(), err)
		return
	}
	b = make([]byte, 100)
	n, err = f.Read(b)
	if n <= 0 || err != nil {
		t.Errorf("%v: Read(100) = %d, %v, want >0, nil", fs.Name(), n, err)
		return
	}
}

func TestOpenFile(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	tmp := GetTmpDir(fs)
	path := filepath.Join(tmp, testName)

	f, err := fs.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Error(fs.Name(), "OpenFile (O_CREATE) failed:", err)
		return
	}
	if _, err := io.WriteString(f, "initial"); err != nil {
		t.Error(fs.Name(), "WriteString failed:", err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(fs.Name(), "Close failed:", err)
		return
	}

	f, err = fs.OpenFile(path, os.O_RDONLY, 0600)
	contents, _ := ioutil.ReadAll(f)
	expectedContents := "initial"
	if string(contents) != expectedContents {
		t.Errorf("%v: writing, expected '%v', got: '%v'", fs.Name(), expectedContents, string(contents))
	}
	f.Close()

	f, err = fs.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Error(fs.Name(), "OpenFile (O_APPEND) failed:", err)
		return
	}
	_, err = io.WriteString(f, "|append")
	if err != nil {
		t.Error(fs.Name(), "WriteString failed:", err)
		return
	}
	f.Close()

	f, err = fs.OpenFile(path, os.O_RDONLY, 0600)
	contents, _ = ioutil.ReadAll(f)
	expectedContents = "initial|append"
	if string(contents) != expectedContents {
		t.Errorf("%v: appending, expected '%v', got: '%v'", fs.Name(), expectedContents, string(contents))
	}
	f.Close()

	f, err = fs.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Error(fs.Name(), "OpenFile (O_TRUNC) failed:", err)
		return
	}
	contents, _ = ioutil.ReadAll(f)
	if string(contents) != "" {
		t.Errorf("%v: expected truncated file, got: '%v'", fs.Name(), string(contents))
	}
	f.Close()
}

func TestCreate(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	tmp := GetTmpDir(fs)
	path := filepath.Join(tmp, testName)

	f, err := fs.Create(path)
	if err != nil {
		t.Error(fs.Name(), "Create failed:", err)
		f.Close()
		return
	}
	io.WriteString(f, "initial")
	f.Close()

	f, err = fs.Create(path)
	if err != nil {
		t.Error(fs.Name(), "Create failed:", err)
		f.Close()
		return
	}
	secondContent := "second create"
	io.WriteString(f, secondContent)
	f.Close()

	f, err = fs.Open(path)
	if err != nil {
		t.Error(fs.Name(), "Open failed:", err)
		f.Close()
		return
	}
	buf, err := kafero.ReadAll(f)
	if err != nil {
		t.Error(fs.Name(), "ReadAll failed:", err)
		f.Close()
		return
	}
	if string(buf) != secondContent {
		t.Error(fs.Name(), "Content should be", "\""+secondContent+"\" but is \""+string(buf)+"\"")
		f.Close()
		return
	}
	f.Close()
}

// TODO what ?
func TestMemFileRead(t *testing.T) {
	f := GetTmpFile(new(kafero.MemMapFs))
	// f := MemFileCreate("testfile")
	f.WriteString("abcd")
	f.Seek(0, 0)
	b := make([]byte, 8)
	n, err := f.Read(b)
	if n != 4 {
		t.Errorf("didn't read all bytes: %v %v %v", n, err, b)
	}
	if err != nil {
		t.Errorf("err is not nil: %v %v %v", n, err, b)
	}
	n, err = f.Read(b)
	if n != 0 {
		t.Errorf("read more bytes: %v %v %v", n, err, b)
	}
	if err != io.EOF {
		t.Errorf("error is not EOF: %v %v %v", n, err, b)
	}
}

func TestRename(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	tDir := GetTmpDir(fs)
	from := filepath.Join(tDir, "/renamefrom")
	to := filepath.Join(tDir, "/renameto")
	exists := filepath.Join(tDir, "/renameexists")
	file, err := fs.Create(from)
	if err != nil {
		t.Fatalf("%s: open %q failed: %v", fs.Name(), to, err)
	}
	if err = file.Close(); err != nil {
		t.Errorf("%s: close %q failed: %v", fs.Name(), to, err)
	}
	file, err = fs.Create(exists)
	if err != nil {
		t.Fatalf("%s: open %q failed: %v", fs.Name(), to, err)
	}
	if err = file.Close(); err != nil {
		t.Errorf("%s: close %q failed: %v", fs.Name(), to, err)
	}
	err = fs.Rename(from, to)
	if err != nil {
		t.Fatalf("%s: rename %q, %q failed: %v", fs.Name(), to, from, err)
	}
	file, err = fs.Create(from)
	if err != nil {
		t.Fatalf("%s: open %q failed: %v", fs.Name(), to, err)
	}
	if err = file.Close(); err != nil {
		t.Errorf("%s: close %q failed: %v", fs.Name(), to, err)
	}
	err = fs.Rename(from, exists)
	if err != nil {
		t.Errorf("%s: rename %q, %q failed: %v", fs.Name(), exists, from, err)
	}
	names, err := kafero.ReadDirNames(fs, tDir)
	if err != nil {
		t.Errorf("%s: readDirNames error: %v", fs.Name(), err)
	}
	found := false
	for _, e := range names {
		if e == "renamefrom" {
			t.Error("File is still called renamefrom")
		}
		if e == "renameto" {
			found = true
		}
	}
	if !found {
		t.Error("File was not renamed to renameto")
	}

	_, err = fs.Stat(to)
	if err != nil {
		t.Errorf("%s: stat %q failed: %v", fs.Name(), to, err)
	}
}

func TestRemove(t *testing.T, fs kafero.Fs) {
	x, err := kafero.TempFile(fs, "", "afero")
	if err != nil {
		t.Error(fmt.Sprint("unable to work with temp file", err))
	}

	path := x.Name()
	x.Close()

	tDir := filepath.Dir(path)

	err = fs.Remove(path)
	if err != nil {
		t.Errorf("%v: Remove() failed: %v", fs.Name(), err)
		return
	}

	_, err = fs.Stat(path)
	if !os.IsNotExist(err) {
		t.Errorf("%v: Remove() didn't remove file", fs.Name())
		return
	}

	// Deleting non-existent file should raise error
	err = fs.Remove(path)
	if !os.IsNotExist(err) {
		t.Errorf("%v: Remove() didn't raise error for non-existent file", fs.Name())
	}

	f, err := fs.Open(tDir)
	if err != nil {
		t.Error("TestDir should still exist:", err)
	}

	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Error("Readdirnames failed:", err)
	}

	for _, e := range names {
		if e == testName {
			t.Error("File was not removed from parent directory")
		}
	}
}

func TestTruncate(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	f := GetTmpFile(fs)
	defer f.Close()

	checkSize(t, f, 0)
	f.Write([]byte("hello, world\n"))
	checkSize(t, f, 13)
	f.Truncate(10)
	checkSize(t, f, 10)
	f.Truncate(1024)
	checkSize(t, f, 1024)
	f.Truncate(0)
	checkSize(t, f, 0)
	_, err := f.Write([]byte("surprise!"))
	if err == nil {
		checkSize(t, f, 13+9) // wrote at offset past where hello, world was.
	}
}

func TestSeek(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	f := GetTmpFile(fs)
	defer f.Close()

	const data = "hello, world\n"
	io.WriteString(f, data)

	type test struct {
		in     int64
		whence int
		out    int64
	}
	var tests = []test{
		{0, 1, int64(len(data))},
		{0, 0, 0},
		{5, 0, 5},
		{0, 2, int64(len(data))},
		{0, 0, 0},
		{-1, 2, int64(len(data)) - 1},
		{1 << 33, 0, 1 << 33},
		{1 << 33, 2, 1<<33 + int64(len(data))},
	}
	for i, tt := range tests {
		off, err := f.Seek(tt.in, tt.whence)
		if off != tt.out || err != nil {
			if e, ok := err.(*os.PathError); ok && e.Err == syscall.EINVAL && tt.out > 1<<32 {
				// Reiserfs rejects the big seeks.
				// http://code.google.com/p/go/issues/detail?id=91
				break
			}
			t.Errorf("#%d: Seek(%v, %v) = %v, %v want %v, nil", i, tt.in, tt.whence, off, err, tt.out)
		}
	}
}

func TestReadAt(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	f := GetTmpFile(fs)
	defer f.Close()

	const data = "hello, world\n"
	if _, err := io.WriteString(f, data); err != nil {
		t.Fatalf("error writing string: %v", err)
	}

	b := make([]byte, 5)
	n, err := f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}
}

func TestWriteAt(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	f := GetTmpFile(fs)

	const data = "hello, world\n"
	io.WriteString(f, data)

	n, err := f.WriteAt([]byte("WORLD"), 7)
	if err != nil || n != 5 {
		t.Fatalf("WriteAt 7: %d, %v", n, err)
	}

	f.Sync()

	f2, err := fs.Open(f.Name())
	if err != nil {
		t.Fatalf("%v: ReadFile %s: %v", fs.Name(), f.Name(), err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(f2)
	b := buf.Bytes()
	if string(b) != "hello, WORLD\n" {
		t.Fatalf("after write: have %q want %q", string(b), "hello, WORLD\n")
	}
	f.Close()
	f2.Close()
}

func TestReadDirNames(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	testSubDir := SetupTestDir(t, fs)
	tDir := filepath.Dir(testSubDir)

	root, err := fs.Open(tDir)
	if err != nil {
		t.Fatal(fs.Name(), tDir, err)
	}
	defer root.Close()

	namesRoot, err := root.Readdirnames(-1)
	if err != nil {
		t.Fatal(fs.Name(), namesRoot, err)
	}

	sub, err := fs.Open(testSubDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	namesSub, err := sub.Readdirnames(-1)
	if err != nil {
		t.Fatal(fs.Name(), namesSub, err)
	}

	findNames(fs, t, tDir, testSubDir, namesRoot, namesSub)
}

func TestReadDirSimple(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)

	testSubDir := SetupTestDir(t, fs)
	tDir := filepath.Dir(testSubDir)

	root, err := fs.Open(tDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	rootInfo, err := root.Readdir(1)
	if err != nil {
		t.Log(myFileInfo(rootInfo))
		t.Error(err)
	}

	rootInfo, err = root.Readdir(5)
	if err != io.EOF {
		t.Log(myFileInfo(rootInfo))
		t.Error(err)
	}

	sub, err := fs.Open(testSubDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	subInfo, err := sub.Readdir(5)
	if err != nil {
		t.Log(myFileInfo(subInfo))
		t.Error(err)
	}
}

func TestReadDir(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	for num := 0; num < 6; num++ {
		var outputs string
		var infos string
		testSubDir := SetupTestDir(t, fs)
		//tDir := filepath.Dir(testSubDir)
		root, err := fs.Open(testSubDir)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()

		for j := 0; j < 6; j++ {
			info, err := root.Readdir(num)
			outputs += fmt.Sprintf("%v  Error: %v\n", myFileInfo(info), err)
			infos += fmt.Sprintln(len(info), err)
		}

		fail := false
		for i, _ := range infos {
			if i == 0 {
				continue
			}
			/*
				TODO
				if o != infos[i-1] {
					fail = true
					break
				}

			*/
		}
		if fail {
			t.Log("Readdir outputs not equal for Readdir(", num, ")")
			for _, o := range outputs {
				t.Log(o)
			}
			t.Fail()
		}
	}
}

// https://github.com/spf13/afero/issues/169
func TestReadDirRegularFiles(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	f := GetTmpFile(fs)
	defer f.Close()

	_, err := f.Readdirnames(-1)
	if err == nil {
		t.Fatal("Expected error")
	}

	_, err = f.Readdir(-1)
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestReadDirAll(t *testing.T, fs kafero.Fs) {
	defer RemoveAllTestFiles(t)
	testSubDir := SetupTestDir(t, fs)
	tDir := filepath.Dir(testSubDir)

	root, err := fs.Open(tDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	rootInfo, err := root.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	var namesRoot = []string{}
	for _, e := range rootInfo {
		namesRoot = append(namesRoot, e.Name())
	}

	sub, err := fs.Open(testSubDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	subInfo, err := sub.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	var namesSub = []string{}
	for _, e := range subInfo {
		namesSub = append(namesSub, e.Name())
	}

	findNames(fs, t, tDir, testSubDir, namesRoot, namesSub)
}

func findNames(fs kafero.Fs, t *testing.T, tDir, testSubDir string, root, sub []string) {
	var foundRoot bool
	for _, e := range root {
		f, err := fs.Open(filepath.Join(tDir, e))
		if err != nil {
			t.Error("Open", filepath.Join(tDir, e), ":", err)
		}
		defer f.Close()

		if equal(e, "we") {
			foundRoot = true
		}
	}
	if !foundRoot {
		t.Logf("Names root: %v", root)
		t.Logf("Names sub: %v", sub)
		t.Error("Didn't find subdirectory we")
	}

	var found1, found2 bool
	for _, e := range sub {
		f, err := fs.Open(filepath.Join(testSubDir, e))
		if err != nil {
			t.Error("Open", filepath.Join(testSubDir, e), ":", err)
		}
		defer f.Close()

		if equal(e, "testfile1") {
			found1 = true
		}
		if equal(e, "testfile2") {
			found2 = true
		}
	}

	if !found1 {
		t.Logf("Names root: %v", root)
		t.Logf("Names sub: %v", sub)
		t.Error("Didn't find testfile1")
	}
	if !found2 {
		t.Logf("Names root: %v", root)
		t.Logf("Names sub: %v", sub)
		t.Error("Didn't find testfile2")
	}
}

type myFileInfo []os.FileInfo

func (m myFileInfo) String() string {
	out := "Fileinfos:\n"
	for _, e := range m {
		out += "  " + e.Name() + "\n"
	}
	return out
}

func RemoveAllTestFiles(t *testing.T) {
	for fs, list := range testRegistry {
		for _, path := range list {
			if err := fs.RemoveAll(path); err != nil {
				t.Error(fs.Name(), err)
			}
		}
	}
	testRegistry = make(map[kafero.Fs][]string)
}

func equal(name1, name2 string) (r bool) {
	switch runtime.GOOS {
	case "windows":
		r = strings.ToLower(name1) == strings.ToLower(name2)
	default:
		r = name1 == name2
	}
	return
}

func checkSize(t *testing.T, f kafero.File, size int64) {
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat %v (looking for size %d): %s", f.Name(), size, err)
	}
	if info.Size() != size {
		t.Errorf("Stat %v: size %d want %d", f.Name(), info.Size(), size)
	}
}

func SetupTestDir(t *testing.T, fs kafero.Fs) string {
	path := GetTmpDir(fs)
	return SetupTestFiles(t, fs, path)
}

func SetupTestDirRoot(t *testing.T, fs kafero.Fs) string {
	path := GetTmpDir(fs)
	SetupTestFiles(t, fs, path)
	return path
}

func SetupTestDirReusePath(t *testing.T, fs kafero.Fs, path string) string {
	testRegistry[fs] = append(testRegistry[fs], path)
	return SetupTestFiles(t, fs, path)
}

func SetupTestFiles(t *testing.T, fs kafero.Fs, path string) string {
	testSubDir := filepath.Join(path, "more", "subdirectories", "for", "testing", "we")
	err := fs.MkdirAll(testSubDir, 0700)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}

	f, err := fs.Create(filepath.Join(testSubDir, "testfile1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("Testfile 1 content"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = fs.Create(filepath.Join(testSubDir, "testfile2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("Testfile 2 content"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = fs.Create(filepath.Join(testSubDir, "testfile3"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("Testfile 3 content"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = fs.Create(filepath.Join(testSubDir, "testfile4"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("Testfile 4 content"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return testSubDir
}
