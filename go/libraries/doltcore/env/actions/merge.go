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

package actions

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)


// TODO: refactor mergo.go to use actions version
func ResolveCommitWithVErr(ddb *doltdb.DoltDB, rsr env.RepoStateReader, cSpecStr string) (*doltdb.Commit, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)

	if err != nil {
		return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
	}

	cm, err := ddb.Resolve(context.TODO(), cs, rsr.CWBHeadRef())

	if err != nil {
		if err == doltdb.ErrInvalidAncestorSpec {
			return nil, errhand.BuildDError("'%s' could not resolve ancestor spec", cSpecStr).Build()
		} else if err == doltdb.ErrBranchNotFound {
			return nil, errhand.BuildDError("unknown branch in commit spec: '%s'", cSpecStr).Build()
		} else if doltdb.IsNotFoundErr(err) {
			return nil, errhand.BuildDError("'%s' not found", cSpecStr).Build()
		} else if err == doltdb.ErrFoundHashNotACommit {
			return nil, errhand.BuildDError("'%s' is not a commit", cSpecStr).Build()
		} else {
			return nil, errhand.BuildDError("Unexpected error resolving '%s'", cSpecStr).AddCause(err).Build()
		}
	}

	return cm, nil
}


// Aborts the current merge checking out all table and clearing the repo merge state.
func AbortMerge(ctx context.Context, rsr env.RepoStateReader, rsw env.RepoStateWriter, fs filesys.Filesys) errhand.VerboseError {
	err := CheckoutAllTables(ctx, rsr, rsw, fs)

	if err == nil {
		err = rsw.ClearMerge()

		if err == nil {
			return nil
		}
	}

	return errhand.BuildDError("fatal: failed to revert changes").AddCause(err).Build()
}

func mapTableHashes(ctx context.Context, root *doltdb.RootValue) (map[string]hash.Hash, error) {
	names, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	nameToHash := make(map[string]hash.Hash)
	for _, name := range names {
		h, ok, err := root.GetTableHash(ctx, name)

		if err != nil {
			return nil, err
		} else if !ok {
			panic("GetTableNames returned a table that GetTableHash says isn't there.")
		} else {
			nameToHash[name] = h
		}
	}

	return nameToHash, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}

// TODO: Move the test case for this here.
func CheckForStompChanges(ctx context.Context, headRoot *doltdb.RootValue, workingRoot *doltdb.RootValue, mergeCommit *doltdb.Commit) ([]string, map[string]hash.Hash, error) {
	mergeRoot, err := mergeCommit.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := mapTableHashes(ctx, headRoot)

	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := mapTableHashes(ctx, workingRoot)

	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mapTableHashes(ctx, mergeRoot)

	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergeWorkingDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergeWorkingDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func ApplyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, errhand.VerboseError) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
		}
	}

	return root, nil
}


