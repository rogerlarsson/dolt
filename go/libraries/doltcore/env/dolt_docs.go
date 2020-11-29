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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type Docs []doltdb.DocDetails

var doltDocsColumns, _ = schema.NewColCollection(
	schema.NewColumn(doltdb.DocPkColumnName, doltdb.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(doltdb.DocTextColumnName, doltdb.DocTextTag, types.StringKind, false),
)
var DoltDocsSchema = schema.MustSchemaFromCols(doltDocsColumns)

// AllValidDocDetails is a list of all valid docs with static fields DocPk and File. All other DocDetail fields
// are dynamic and must be added, modified or removed as needed.
var AllValidDocDetails = &Docs{
	doltdb.DocDetails{DocPk: doltdb.ReadmePk, File: ReadmeFile},
	doltdb.DocDetails{DocPk: doltdb.LicensePk, File: LicenseFile},
}

var validDocs = map[string]string{
	doltdb.ReadmePk: ReadmeFile,
	doltdb.LicensePk: LicenseFile,
}


func LoadDocs(fs filesys.ReadWriteFS) (Docs, error) {
	docsWithCurrentText := *AllValidDocDetails
	for i, val := range docsWithCurrentText {
		path := getDocFile(val.File)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			data, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}
			val.NewerText = data
			docsWithCurrentText[i] = val
		}
	}
	return docsWithCurrentText, nil
}

func (docs Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range docs {
		if !IsValidDoc(doc.DocPk) {
			continue
		}
		filePath := getDocFile(doc.File)
		if doc.NewerText != nil {
			err := fs.WriteFile(filePath, doc.NewerText)
			if err != nil {
				return err
			}
		} else {
			err := DeleteDoc(fs, doc.DocPk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func DeleteDoc(fs filesys.ReadWriteFS, docName string) error {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			path := getDocFile(doc.File)
			exists, isDir := fs.Exists(path)
			if exists && !isDir {
				return fs.DeleteFile(path)
			}
		}
	}
	return nil
}

func IsValidDoc(docName string) bool {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			return true
		}
	}
	return false
}

func hasDocFile(fs filesys.ReadWriteFS, file string) bool {
	exists, isDir := fs.Exists(getDocFile(file))
	return exists && !isDir
}


func SyncDocsFromFS(ctx context.Context, dEnv *DoltEnv) error {
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