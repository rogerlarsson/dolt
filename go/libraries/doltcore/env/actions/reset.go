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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

func ResetHard(ctx context.Context, dEnv *env.DoltEnv, headRoot *doltdb.RootValue) error {
	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	staged, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return err
	}

	// need to save the state of tables that aren't tracked
	untrackedTbls, err := getUntrackedTables(ctx, working, staged)
	if err != nil {
		return err
	}

	// newWorking = headRoot + untracked working tables
	newWorking, err := MoveTablesBetweenRoots(ctx, untrackedTbls, working, headRoot)
	if err != nil {
		return err
	}

	untrackedDocs, err := getUntrackedDocs(ctx, working, staged)
	if err != nil {
		return err
	}

	newWorking, err = MoveDocsBetweenRoots(ctx, untrackedDocs, working, newWorking)
	if err != nil {
		return err
	}

	return saveRepoState(ctx, dEnv, newWorking, headRoot)
}

func getUntrackedTables(ctx context.Context, working, staged *doltdb.RootValue) ([]string, error) {
	workingNames, err := working.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	untracked := set.NewStrSet(workingNames)

	// tables in the staged root are tracked
	stagedNames, err := staged.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	untracked.Remove(stagedNames...)

	return untracked.AsSlice(), nil
}

func getUntrackedDocs(ctx context.Context, working, staged *doltdb.RootValue) ([]string, error) {
	workingNames, err := doltdb.GetDocNames(ctx, working)
	if err != nil {
		return nil, err
	}
	untracked := set.NewStrSet(workingNames)

	// tables in the staged root are tracked
	stagedNames, err := doltdb.GetDocNames(ctx, staged)
	if err != nil {
		return nil, err
	}
	untracked.Remove(stagedNames...)

	return untracked.AsSlice(), nil
}

func ResetSoft(ctx context.Context, dEnv *env.DoltEnv, names []string) error {
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return err
	}

	tables, docs := splitTablesAndDocs(names)

	stagedRoot, err = MoveDocsBetweenRoots(ctx, docs, headRoot, stagedRoot)
	if err != nil {
		return err
	}

	err = ValidateTables(ctx, tables, stagedRoot, headRoot)
	if err != nil {
		return err
	}

	stagedRoot, err = MoveTablesBetweenRoots(ctx, tables, headRoot, stagedRoot)
	if err != nil {
		return err
	}

	_, err = dEnv.UpdateStagedRoot(ctx, stagedRoot)
	return err
}

