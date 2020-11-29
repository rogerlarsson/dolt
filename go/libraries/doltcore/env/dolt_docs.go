// Copyright 2019 Dolthub, Inc.
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

package env

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var validDocs = map[string]string{
	doltdb.ReadmePk: ReadmeFile,
	doltdb.LicensePk: LicenseFile,
}

func SyncDocsFromFS(ctx context.Context, dEnv *DoltEnv) error {
	// check if initialized
	if !dEnv.HasDoltDataDir() {
		return nil
	}

	docs, err := readDocs(dEnv.FS)
	if err != nil {
		return err
	}

	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	working, err = doltdb.PutDocs(ctx, docs, working)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingRoot(ctx, working)
}

func readDocs(fs filesys.ReadableFS) (docs map[string]string, err error) {
	docs = make(map[string]string)
	for name, file := range validDocs {
		path := getDocFile(file)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			text, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}

			docs[name] = string(text)
		}
	}
	return docs, nil
}

func SyncDocsToFS(ctx context.Context, dEnv *DoltEnv) error {
	// check if initialized
	if !dEnv.HasDoltDataDir() {
		return nil
	}

	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	docs, err := doltdb.ReadDocs(ctx, working)
	if err != nil {
		return err
	}

	return writeDocs(dEnv.FS, docs)
}

func writeDocs(fs filesys.WritableFS, docs map[string]string) (err error) {
	for name, text := range docs {
		err = fs.WriteFile(name, []byte(text))
		if err != nil {
			break
		}
	}
	return err
}