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

package dfunctions

import (
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/net/context"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	children []sql.Expression
}

// Refactor these params
const (
	abortParam  = "abort"
	squashParam = "squash"
	noFFParam   = "no-ff"
)

// TODO Refactor from commands/merge.go
func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag("abort", "", "abort this thing")
	ap.SupportsFlag("squash", "", "Merges changes to the working set without updating the commit history")
	ap.SupportsFlag("no-ff", "", "Create a merge commit even when the merge resolves as a fast-forward.")
	ap.SupportsString(cli.CommitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	return ap
}

func (d DoltMergeFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltMergeFunc) String() string {
	return fmt.Sprintf("commit_hash")
}

func (d DoltMergeFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltMergeFunc) IsNullable() bool {
	return false
}

func (d DoltMergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)

	_, rsr, rsw, err := getDdbRswRsrFromSession(dSess, dbName)

	if err != nil {
		return nil, err
	}

	ap := createArgParser()

	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return "", err
	}

	apr := cli.ParseArgs(ap, args, nil)

	if apr.ContainsAll("squash", "no-ff") {
		return nil, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", "squash", "no-ff")
	}

	if apr.Contains(abortParam) {
		if !rsr.IsMergeActive() {
			return nil, fmt.Errorf("fatal: There is no merge to abort")
		}
		err = actions.AbortMerge(ctx, rsr, rsw, nil)
	} else {
		if apr.NArg() != 1 {
			return "", fmt.Errorf("Incorrect usage. Be sure to include a branch.")
		}

		commitSpecStr := apr.Arg(0)

		var working *doltdb.RootValue
		working, err := rsr.WorkingRoot(ctx)

		if err == nil {
			if has, err := working.HasConflicts(ctx); err != nil {
				err = errhand.BuildDError("error: failed to get conflicts").AddCause(err).Build()
			} else if has {
				return nil, fmt.Errorf("error: Merging is not possible because you have unmerged files.")
			} else if rsr.IsMergeActive() {
				return nil, fmt.Errorf("error: Merging is not possible because you have not committed an active merge.")
			}

			if err == nil {
				err = mergeCommitSpec(ctx, dSess, dbName, apr, commitSpecStr)
			}
		}
	}

	// TODO: Change this fam
	return "change this fam", err
}

// TODO: Remove all build errors
func mergeCommitSpec(ctx *sql.Context, dSess *sqle.DoltSession, dbName string, apr *argparser.ArgParseResults,
	 				 commitSpecStr string) error {
	ddb, rsr, rsw, err := getDdbRswRsrFromSession(dSess, dbName)

	if err != nil {
		return err
	}

	cm1, err := actions.ResolveCommitWithVErr(ddb, rsr, "HEAD")

	if err != nil {
		return fmt.Errorf(err.Error())
	}

	cm2, err := actions.ResolveCommitWithVErr(ddb, rsr, commitSpecStr)

	if err != nil {
		return fmt.Errorf(err.Error())
	}

	h1, err := cm1.HashOf()

	if err != nil {
		return err
	}

	h2, err := cm2.HashOf()

	if err != nil {
		return err
	}

	if h1 == h2 {
		return fmt.Errorf("Everything up-to-date")
	}

	cli.Println("Updating", h1.String()+".."+h2.String())

	squash := apr.Contains(squashParam)
	if squash {
		cli.Println("Squash commit -- not updating HEAD")
	}

	headRoot, err := rsr.HeadRoot(ctx)

	if err != nil {
		return err
	}

	workingRoot, err := rsr.WorkingRoot(ctx)

	if err != nil {
		return err
	}
	tblNames, workingDiffs, err := actions.CheckForStompChanges(ctx, headRoot, workingRoot, cm2)

	if err != nil {
		return err
	}

	if len(tblNames) != 0 {
		bldr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by merge:")
		for _, tName := range tblNames {
			bldr.AddDetails(tName)
		}
		bldr.AddDetails("Please commit your changes before you merge.")
		return fmt.Errorf(bldr.Build().Error())
	}

	if ok, err := cm1.CanFastForwardTo(ctx, cm2); ok {
		if apr.Contains(noFFParam) {
			return execNoFFMerge(ctx, apr, dSess, dbName, cm2, workingDiffs)
		} else {
			return executeFFMerge(ctx, squash, ddb, rsr, rsw, cm2, workingDiffs)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
		return nil
	} else {
		return executeMerge(ctx, squash, rsw, cm1, cm2, workingDiffs)
	}
}

func execNoFFMerge(ctx *sql.Context, apr *argparser.ArgParseResults, dSess *sqle.DoltSession, dbName string,
				   cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) error {
	ddb, rsr, rsw, err := getDdbRswRsrFromSession(dSess, dbName)

	if err != nil {
		return err
	}

	mergedRoot, err := cm2.GetRootValue()

	if err != nil {
		return err
	}

	err = mergedRootToWorking(ctx, false, rsw, mergedRoot, workingDiffs, cm2, map[string]*merge.MergeStats{})

	if err != nil {
		return err
	}

	_, err = prepareCommit(ctx, apr, dSess, ddb, rsr, rsw)

	return err
}


func executeFFMerge(ctx context.Context, squash bool, ddb *doltdb.DoltDB, rsr env.RepoStateReader, rsw env.RepoStateWriter,
					cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) error {
	cli.Println("Fast-forward")

	rv, err := cm2.GetRootValue()

	if err != nil {
		return err
	}

	stagedHash, err := ddb.WriteRootValue(ctx, rv)
	if err != nil {
		return err
	}

	workingHash := stagedHash
	if len(workingDiffs) > 0 {
		rv, err = actions.ApplyChanges(ctx, rv, workingDiffs)

		if err != nil {
			return err
		}

		workingHash, err = ddb.WriteRootValue(ctx, rv)

		if err != nil {
			return err
		}
	}

	if !squash {
		err = ddb.FastForward(ctx, rsr.CWBHeadRef(), cm2)

		if err != nil {
			return err
		}
	}

	rsw.SetWorkingHash(ctx, workingHash)
	rsw.SetStagedHash(ctx, stagedHash)

	if err != nil {
		return err
	}

	return nil
}


func executeMerge(ctx context.Context, squash bool, rsw env.RepoStateWriter, cm1,
	   			  cm2 *doltdb.Commit, workingDiffs map[string]hash.Hash) error {
	mergedRoot, tblToStats, err := merge.MergeCommits(ctx, cm1, cm2)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return fmt.Errorf("Already up to date.")
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return err
		}
	}

	return mergedRootToWorking(ctx, squash, rsw, mergedRoot, workingDiffs, cm2, tblToStats)
}

func mergedRootToWorking(ctx context.Context, squash bool, rsw env.RepoStateWriter,
						 mergedRoot *doltdb.RootValue, workingDiffs map[string]hash.Hash, cm2 *doltdb.Commit,
						 tblToStats map[string]*merge.MergeStats) error {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = actions.ApplyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			return err
		}
	}

	h2, err := cm2.HashOf()

	if err != nil {
		return err
	}

	if !squash {
		err = rsw.StartMerge(h2)

		if err != nil {
			return err
		}
	}

	err = updateWorkingWithErr(rsw, workingRoot)

	if err == nil {
		hasConflicts := hasMergeConflicts(tblToStats)

		if hasConflicts {
			err = fmt.Errorf("Automatic merge failed; fix conflicts and then commit the result.")
		} else {
			err = updateStagedWithErr(rsw, mergedRoot)
			if err != nil {
				// Log a new message here to indicate that merge was successful, only staging failed.
				cli.Println("Unable to stage changes: add and commit to finish merge")
			}
		}
	}

	return err
}

func getDdbRswRsrFromSession(dSess *sqle.DoltSession, dbName string) (*doltdb.DoltDB, env.RepoStateReader, env.RepoStateWriter, error) {
	ddb, ok := dSess.GetDoltDB(dbName)

	if !ok {
		return nil, nil, nil, fmt.Errorf("Could not load %s", dbName)
	}

	rsr, ok := dSess.GetDoltDBRepoStateReader(dbName)

	if !ok {
		return nil, nil, nil, fmt.Errorf("Could not load the %s RepoStateReader", dbName)
	}

	rsw, ok := dSess.GetDoltDBRepoStateWriter(dbName)

	if !ok {
		return nil, nil, nil, fmt.Errorf("Could not load the %s RepoStateWriter", dbName)
	}

	return ddb, rsr, rsw, nil
}

func updateWorkingWithErr(rsw env.RepoStateWriter, updatedRoot *doltdb.RootValue) error {
	err := rsw.UpdateWorkingRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return fmt.Errorf("fatal: failed to write value")
	case env.ErrStateUpdate:
		return fmt.Errorf("fatal: failed to update the working root state")
	}

	return nil
}

func updateStagedWithErr(rsw env.RepoStateWriter, updatedRoot *doltdb.RootValue) error {
	_, err := rsw.UpdateStagedRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return fmt.Errorf("fatal: failed to write value")
	case env.ErrStateUpdate:
		return fmt.Errorf("fatal: failed to update the staged root state")
	}

	return nil
}

func hasMergeConflicts(tblToStats map[string]*merge.MergeStats) bool {
	hasConflicts := false
	for _, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts > 0 {
			hasConflicts = true
		}
	}

	return hasConflicts
}


func (d DoltMergeFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltMergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(children...)
}

// NewDoltMergeFunc creates a new DoltMergeFunc expression whose children represents the args passed in DOLT_MERGE.
func NewDoltMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{children: args}, nil
}

