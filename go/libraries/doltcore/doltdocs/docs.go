// Copyright 2020 Dolthub, Inc.
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

package doltdocs

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ReadmeFile  = "../README.md"
	LicenseFile = "../LICENSE.md"
)

type Doc struct {
	Text  []byte
	DocPk string
	File  string
}

type Docs []Doc

var SupportedDocs = &Docs{
	{DocPk: doltdb.ReadmePk, File: ReadmeFile},
	{DocPk: doltdb.LicensePk, File: LicenseFile},
}

// GetDocFilePath takes in a filename and appends it to the DoltDir filepath.
func GetDocFilePath(filename string) string {
	return filepath.Join(dbfactory.DoltDir, filename)
}

// LoadDocs takes in a fs object and reads all the docs (ex. README.md) defined in SupportedDocs.
func LoadDocs(fs filesys.ReadWriteFS) (Docs, error) {
	docsWithCurrentText := *SupportedDocs
	for i, val := range docsWithCurrentText {
		path := GetDocFilePath(val.File)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			data, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}
			val.Text = data
			docsWithCurrentText[i] = val
		}
	}
	return docsWithCurrentText, nil
}

// Save takes in a fs object and saves all the docs to the filesystem, overwriting any existing files.
func (docs Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range docs {
		if _, ok := IsSupportedDoc(doc.DocPk); !ok {
			continue
		}
		filePath := GetDocFilePath(doc.File)
		if doc.Text != nil {
			err := fs.WriteFile(filePath, doc.Text)
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

// DeleteDoc takes in a filesytem object and deletes the file with docName, if it's a SupportedDoc.
func DeleteDoc(fs filesys.ReadWriteFS, docName string) error {
	if doc, ok := IsSupportedDoc(docName); ok {
		if doc.DocPk == docName {
			path := GetDocFilePath(doc.File)
			exists, isDir := fs.Exists(path)
			if exists && !isDir {
				return fs.DeleteFile(path)
			}
		}
	}
	return nil
}

func IsSupportedDoc(docName string) (Doc, bool) {
	for _, doc := range *SupportedDocs {
		if doc.DocPk == docName {
			return doc, true
		}
	}
	return Doc{}, false
}

func docFileExists(fs filesys.ReadWriteFS, file string) bool {
	exists, isDir := fs.Exists(GetDocFilePath(file))
	return exists && !isDir
}

// GetLocalFileText returns a byte slice representing the contents of the provided file, if it exists
func GetLocalFileText(fs filesys.Filesys, file string) ([]byte, error) {
	path := ""
	if docFileExists(fs, file) {
		path = GetDocFilePath(file)
	}

	if path != "" {
		return fs.ReadFile(path)
	}

	return nil, nil
}

// GetSupportedDocs takes in a filesystem and returns the contents of all docs on disk.
func GetSupportedDocs(fs filesys.Filesys) (docs Docs, err error) {
	docs = Docs{}
	for _, doc := range *SupportedDocs {
		newerText, err := GetLocalFileText(fs, doc.File)
		if err != nil {
			return nil, err
		}
		doc.Text = newerText
		docs = append(docs, doc)
	}
	return docs, nil
}

// GetDoc takes in a filesystem and a docName and returns the doc's contents.
func GetDoc(fs filesys.Filesys, docName string) (doc Doc, err error) {
	for _, doc := range *SupportedDocs {
		if doc.DocPk == docName {
			newerText, err := GetLocalFileText(fs, doc.File)
			if err != nil {
				return Doc{}, err
			}
			doc.Text = newerText
			return doc, nil
		}
	}
	return Doc{}, err
}

func DocTblKeyFromName(fmt *types.NomsBinFormat, name string) (types.Tuple, error) {
	return types.NewTuple(fmt, types.Uint(schema.DocNameTag), types.String(name))
}

// GetDocRow returns the associated row of a particular doc from the docTbl given.
func GetDocRow(ctx context.Context, docTbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
	rowMap, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}

	var fields types.Value
	fields, ok, err = rowMap.MaybeGet(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}

	r, err = row.FromNoms(sch, key, fields.(types.Tuple))
	return r, ok, err
}

// GetDocTextFromTbl returns the Text field of a doc using the provided table and schema and primary key.
func GetDocTextFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docPk string) ([]byte, error) {
	if tbl != nil && sch != nil {
		key, err := DocTblKeyFromName(tbl.Format(), docPk)
		if err != nil {
			return nil, err
		}

		docRow, ok, err := GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return nil, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			return []byte(docValue.(types.String)), nil
		} else {
			return nil, nil
		}
	} else {
		return nil, nil
	}
}

// GetDocTextFromRow updates return the text field of a provided row.
func GetDocTextFromRow(r row.Row) ([]byte, error) {
	docValue, ok := r.GetColVal(schema.DocTextTag)
	if !ok {
		return nil, nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return nil, err
		}
		return []byte(docValStr), nil
	}
}

// GetDocPKFromRow updates returns the docPk field of a given row.
func GetDocPKFromRow(r row.Row) (string, error) {
	colVal, _ := r.GetColVal(schema.DocNameTag)
	if colVal == nil {
		return "", nil
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return "", err
		}

		return docName, nil
	}
}

func GetDocNamesFromDocs(docs Docs) []string {
	if docs == nil {
		return nil
	}

	ret := make([]string, len(docs))

	for i, doc := range docs {
		ret[i] = doc.DocPk
	}

	return ret
}

// GetDocsFromRoot takes in a root value and returns the docs stored in its dolt_docs table.
func GetDocsFromRoot(ctx context.Context, root *doltdb.RootValue, docNames []string) (Docs, error) {
	docTbl, docTblFound, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, err
	}

	var sch schema.Schema
	if docTblFound {
		docSch, err := docTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		sch = docSch
	}

	if docNames == nil {
		docNames = GetDocNamesFromDocs(*SupportedDocs)
	}

	docs := make(Docs, len(docNames))
	for i, name := range docNames {
		doc, isSupported := IsSupportedDoc(name)
		if !isSupported {
			return nil, fmt.Errorf("%s is not a supported doc", name)
		}

		docText, err := GetDocTextFromTbl(ctx, docTbl, &sch, name)
		if err != nil {
			return nil, err
		}

		doc.Text = docText
		docs[i] = doc
	}

	return docs, nil
}
