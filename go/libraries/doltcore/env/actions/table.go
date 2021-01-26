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

package actions

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	roots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])

	if err != nil {
		return err
	}

	docs := doltdocs.SupportedDocs

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)

}

func CheckoutTables(ctx context.Context, dbData env.DbData, tables []string) error {
	roots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTables(ctx, dbData, roots, tables)
}

func CheckoutDocs(ctx context.Context, dbData env.DbData, docs doltdocs.Docs) error {
	roots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutDocs(ctx, dbData, roots, docs)
}

// MoveTablesBetweenRoots copies tables with names in tbls from the src RootValue to the dest RootValue.
// It matches tables between roots by column tags.
func MoveTablesBetweenRoots(ctx context.Context, tbls []string, src, dest *doltdb.RootValue) (*doltdb.RootValue, error) {
	tblSet := set.NewStrSet(tbls)

	stagedFKs, err := dest.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	tblDeltas, err := diff.GetTableDeltas(ctx, dest, src)
	if err != nil {
		return nil, err
	}

	tblsToDrop := set.NewStrSet(nil)

	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !tblSet.Contains(td.FromName) {
				continue
			}

			tblsToDrop.Add(td.FromName)
		} else {
			if !tblSet.Contains(td.ToName) {
				continue
			}

			if td.IsRename() {
				// rename table before adding the new version so we don't have
				// two copies of the same table
				dest, err = dest.RenameTable(ctx, td.FromName, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			dest, err = dest.PutTable(ctx, td.ToName, td.ToTable)
			if err != nil {
				return nil, err
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			err = stagedFKs.AddKeys(td.ToFks...)
			if err != nil {
				return nil, err
			}

			ss, _, err := src.GetSuperSchema(ctx, td.ToName)
			if err != nil {
				return nil, err
			}

			dest, err = dest.PutSuperSchema(ctx, td.ToName, ss)
			if err != nil {
				return nil, err
			}
		}
	}

	dest, err = dest.PutForeignKeyCollection(ctx, stagedFKs)
	if err != nil {
		return nil, err
	}

	// RemoveTables also removes that table's ForeignKeys
	dest, err = dest.RemoveTables(ctx, tblsToDrop.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return dest, nil
}

func checkoutTables(ctx context.Context, dbData env.DbData, roots map[RootType]*doltdb.RootValue, tbls []string) error {
	unknownTbls := []string{}

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]

	for _, tblName := range tbls {
		if tblName == doltdb.DocTableName {
			continue
		}
		tbl, ok, err := staged.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		if !ok {
			tbl, ok, err = head.GetTable(ctx, tblName)

			if err != nil {
				return err
			}

			if !ok {
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		currRoot, err = currRoot.PutTable(ctx, tblName, tbl)

		if err != nil {
			return err
		}
	}

	if len(unknownTbls) > 0 {
		// Return table not exist error before RemoveTables, which fails silently if the table is not on the root.
		err := validateTablesExist(ctx, currRoot, unknownTbls)
		if err != nil {
			return err
		}

		currRoot, err = currRoot.RemoveTables(ctx, unknownTbls...)

		if err != nil {
			return err
		}
	}

	// update the working root with currRoot
	_, err := env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, currRoot)

	return err
}

func checkoutDocs(ctx context.Context, dbData env.DbData, roots map[RootType]*doltdb.RootValue, docs doltdocs.Docs) error {
	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]

	if len(docs) > 0 {
		currRootWithDocs, stagedWithDocs, updatedDocs, err := getUpdatedWorkingAndStagedWithDocs(ctx, currRoot, staged, head, docs)
		if err != nil {
			return err
		}
		currRoot = currRootWithDocs
		staged = stagedWithDocs
		docs = updatedDocs
	}

	_, err := env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, currRoot)
	if err != nil {
		return err
	}

	return dbData.Drw.WriteDocsToDisk(docs)
}

func checkoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, roots map[RootType]*doltdb.RootValue, tbls []string, docs doltdocs.Docs) error {
	unknownTbls := []string{}

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]

	if len(docs) > 0 {
		currRootWithDocs, stagedWithDocs, updatedDocs, err := getUpdatedWorkingAndStagedWithDocs(ctx, currRoot, staged, head, docs)
		if err != nil {
			return err
		}
		currRoot = currRootWithDocs
		staged = stagedWithDocs
		docs = updatedDocs
	}

	for _, tblName := range tbls {
		if tblName == doltdb.DocTableName {
			continue
		}
		tbl, ok, err := staged.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		if !ok {
			tbl, ok, err = head.GetTable(ctx, tblName)

			if err != nil {
				return err
			}

			if !ok {
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		currRoot, err = currRoot.PutTable(ctx, tblName, tbl)

		if err != nil {
			return err
		}
	}

	if len(unknownTbls) > 0 {
		// Return table not exist error before RemoveTables, which fails silently if the table is not on the root.
		err := validateTablesExist(ctx, currRoot, unknownTbls)
		if err != nil {
			return err
		}

		currRoot, err = currRoot.RemoveTables(ctx, unknownTbls...)

		if err != nil {
			return err
		}
	}

	err := dEnv.UpdateWorkingRoot(ctx, currRoot)
	if err != nil {
		return err
	}

	return dEnv.DocsReadWriter().WriteDocsToDisk(docs)
}

func validateTablesExist(ctx context.Context, currRoot *doltdb.RootValue, unknown []string) error {
	notExist := []string{}
	for _, tbl := range unknown {
		if has, err := currRoot.HasTable(ctx, tbl); err != nil {
			return err
		} else if !has {
			notExist = append(notExist, tbl)
		}
	}

	if len(notExist) > 0 {
		return NewTblNotExistError(notExist)
	}

	return nil
}

// RemoveDocsTable takes a slice of table names and returns a new slice with DocTableName removed.
func RemoveDocsTable(tbls []string) []string {
	var result []string
	for _, tblName := range tbls {
		if tblName != doltdb.DocTableName {
			result = append(result, tblName)
		}
	}
	return result
}


// GetRemoteBranchRef returns the ref of a branch and ensures it matched with name
func GetRemoteBranchRef(ctx context.Context, ddb *doltdb.DoltDB, name string) (ref.DoltRef, bool, error) {
	remoteRefFilter := map[ref.RefType]struct{}{ref.RemoteRefType: {}}
	refs, err := ddb.GetRefsOfType(ctx, remoteRefFilter)

	if err != nil {
		return nil, false, err
	}

	for _, rf := range refs {
		if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == name {
			return rf, true, nil
		}
	}

	return nil, false, err
}
