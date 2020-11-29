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

package doltdb

import (
	"context"
	"io"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// LicensePk is the key for accessing the license within the docs table
	LicensePk = "LICENSE.md"
	// ReadmePk is the key for accessing the readme within the docs table
	ReadmePk = "README.md"
)

type DocDetails struct {
	NewerText []byte
	DocPk     string
	Value     types.Value
	File      string
}

var DocNameSet = set.NewStrSet([]string{
	LicensePk,
	ReadmePk,
})

func UnionDocNames(ctx context.Context, roots ...*RootValue) ([]string, error) {
	union := set.NewStrSet(nil)
	for _, root := range roots {
		names, err := GetDocNames(ctx, root)
		if err != nil {
			return nil, err
		}

		for _, name := range names {
			union.Add(name)
		}
	}

	return union.AsSlice(), nil
}

func GetDocNames(ctx context.Context, root *RootValue) ([]string, error) {
	docs, err := ReadDocs(ctx, root)
	if err != nil {
		return nil, err
	}

	var names []string
	for name := range docs {
		names = append(names, name)
	}

	return names, nil
}

func PutDocs(ctx context.Context, docs map[string]string, root *RootValue) (*RootValue, error) {
	tbl, ok, err := root.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return root, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	format := root.VRW().Format()
	editor := rows.Edit()
	for name, text := range docs {
		pk := row.TaggedValues{
			DocNameTag: types.String(name),
			DocTextTag: types.String(text),
		}

		pkVal, err := pk.NomsTupleForPKCols(format, sch.GetPKCols()).Value(ctx)
		if err != nil {
			return nil, err
		}

		txtVal, err := pk.NomsTupleForNonPKCols(format, sch.GetNonPKCols()).Value(ctx)
		if err != nil {
			return nil, err
		}

		editor.Set(pkVal, txtVal)
	}

	rows, err = editor.Map(ctx)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.UpdateRows(ctx, rows)
	if err != nil {
		return nil, err
	}

	return root.PutTable(ctx, DocTableName, tbl)
}

func RemoveDocs(ctx context.Context, docNames *set.StrSet, root *RootValue) (*RootValue, error) {
	tbl, ok, err := root.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return root, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	format := root.VRW().Format()
	editor := rows.Edit()
	for _, name := range docNames.AsSlice() {
		pk := row.TaggedValues{
			DocNameTag: types.String(name),
		}

		val, err := pk.NomsTupleForPKCols(format, sch.GetPKCols()).Value(ctx)
		if err != nil {
			return nil, err
		}

		editor.Remove(val)
	}

	rows, err = editor.Map(ctx)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.UpdateRows(ctx, rows)
	if err != nil {
		return nil, err
	}

	return root.PutTable(ctx, DocTableName, tbl)
}

func ReadDocs(ctx context.Context, root *RootValue) (docs map[string]string, err error) {
	docs = make(map[string]string)

	tbl, ok, err := root.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return docs, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	rdr, err := noms.NewNomsMapReader(ctx, rows, sch)
	if err != nil {
		return nil, err
	}

	for {
		r, err := rdr.ReadRow(ctx)
		if err != nil {
			break
		}

		docName, _ := r.GetColVal(DocNameTag)
		docText, _ := r.GetColVal(DocTextTag)
		docs[string(docName.(types.String))] = string(docText.(types.String))
	}
	if err == io.EOF {
		err = nil
	}

	return docs, err
}






func getDocDetailsBtwnRoots(ctx context.Context, newTbl *Table, newSch schema.Schema, newTblFound bool, oldTbl *Table, oldSch schema.Schema, oldTblFound bool) ([]DocDetails, error) {
	var docDetailsBtwnRoots []DocDetails
	if newTblFound {
		newRows, err := newTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = newRows.IterAll(ctx, func(key, val types.Value) error {
			newRow, err := row.FromNoms(newSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := DocDetails{}
			updated, err := addDocPKToDocFromRow(newRow, &doc)
			if err != nil {
				return err
			}
			updated, err = addNewerTextToDocFromRow(ctx, newRow, &updated)
			if err != nil {
				return err
			}
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if oldTblFound {
		oldRows, err := oldTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = oldRows.IterAll(ctx, func(key, val types.Value) error {
			oldRow, err := row.FromNoms(oldSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := DocDetails{}
			updated, err := addDocPKToDocFromRow(oldRow, &doc)
			if err != nil {
				return err
			}
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			updated, err = AddNewerTextToDocFromTbl(ctx, newTbl, &newSch, updated)
			if err != nil {
				return err
			}

			if updated.Value != nil && updated.NewerText == nil {
				docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return docDetailsBtwnRoots, nil
}

func GetDocDiffsFromDocDetails(ctx context.Context, docDetails []DocDetails) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}
	for _, doc := range docDetails {
		added, modified, removed = appendDocDiffs(added, modified, removed, doc.Value, doc.NewerText, doc.DocPk)
	}
	return added, modified, removed
}

func addValuesToDocs(ctx context.Context, tbl *Table, sch *schema.Schema, docDetails []DocDetails) ([]DocDetails, error) {
	if tbl != nil && sch != nil {
		for i, details := range docDetails {
			newDetails, err := AddValueToDocFromTbl(ctx, tbl, sch, details)
			if err != nil {
				return nil, err
			}
			docDetails[i] = newDetails
		}
	}
	return docDetails, nil
}

// AddValueToDocFromTbl updates the Value field of a docDetail using the provided table and schema.
func AddValueToDocFromTbl(ctx context.Context, tbl *Table, sch *schema.Schema, docDetail DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		pkTaggedVal := row.TaggedValues{
			DocNameTag: types.String(docDetail.DocPk),
		}

		docRow, ok, err := tbl.GetRowByPKVals(ctx, pkTaggedVal, *sch)
		if err != nil {
			return DocDetails{}, err
		}

		if ok {
			docValue, _ := docRow.GetColVal(DocTextTag)
			docDetail.Value = docValue
		} else {
			docDetail.Value = nil
		}
	} else {
		docDetail.Value = nil
	}
	return docDetail, nil
}

// AddNewerTextToDocFromTbl updates the NewerText field of a docDetail using the provided table and schema.
func AddNewerTextToDocFromTbl(ctx context.Context, tbl *Table, sch *schema.Schema, doc DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		pkTaggedVal := row.TaggedValues{
			DocNameTag: types.String(doc.DocPk),
		}

		docRow, ok, err := tbl.GetRowByPKVals(ctx, pkTaggedVal, *sch)
		if err != nil {
			return DocDetails{}, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(DocTextTag)
			doc.NewerText = []byte(docValue.(types.String))
		} else {
			doc.NewerText = nil
		}
	} else {
		doc.NewerText = nil
	}
	return doc, nil
}

func addNewerTextToDocFromRow(ctx context.Context, r row.Row, doc *DocDetails) (DocDetails, error) {
	docValue, ok := r.GetColVal(DocTextTag)
	if !ok {
		doc.NewerText = nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.NewerText = []byte(docValStr)
	}
	return *doc, nil
}

func addDocPKToDocFromRow(r row.Row, doc *DocDetails) (DocDetails, error) {
	colVal, _ := r.GetColVal(DocNameTag)
	if colVal == nil {
		doc.DocPk = ""
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return DocDetails{}, err
		}
		doc.DocPk = docName
	}

	return *doc, nil
}

func appendDocDiffs(added, modified, removed []string, olderVal types.Value, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if olderVal.HumanReadableString() != strconv.Quote(string(newerVal)) {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
}
