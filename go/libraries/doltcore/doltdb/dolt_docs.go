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
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
	"io"
)

const (
	// LicensePk is the key for accessing the license within the docs table
	LicensePk = "LICENSE.md"
	// ReadmePk is the key for accessing the readme within the docs table
	ReadmePk = "README.md"
)

var DocNameSet = set.NewStrSet([]string{
	LicensePk,
	ReadmePk,
})

var DoltDocsSchema = schema.MustSchemaFromCols(doltDocsColumns)

var doltDocsColumns, _ = schema.NewColCollection(
	schema.NewColumn(DocPkColumnName, DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(DocTextColumnName, DocTextTag, types.StringKind, false),
)

type DocDetails struct {
	NewerText []byte
	DocPk     string
	Value     types.Value
	File      string
}

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
	root, tbl, err := getOrCreateDocsTable(ctx, root)
	if err != nil {
		return nil, err
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

func getOrCreateDocsTable(ctx context.Context, root *RootValue) (*RootValue, *Table, error) {
	ok, err := root.HasTable(ctx, DocTableName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		root, err = root.CreateEmptyTable(ctx, DocTableName, DoltDocsSchema)
		if err != nil {
			return nil, nil, err
		}
	}

	tbl, _, err := root.GetTable(ctx, DocTableName)
	if err != nil {
		return nil, nil, err
	}

	return root, tbl, nil
}

