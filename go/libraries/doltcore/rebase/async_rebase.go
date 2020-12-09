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

package rebase

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

// AsyncReplayRootFn is a ReplayRootFn function that doesn't rely on the rebasedParent, and can be executing concurrently.
// It defines an operation that produces a rebase version of |root|.
type AsyncReplayRootFn func(ctx context.Context, root, parentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error)

// AsyncReplayCommitFn is a ReplayCommitFn function that doesn't rely on the rebasedParent, and can be executing concurrently.
// It defines an operation that produces a rebase version of |commit|.
type AsyncReplayCommitFn func(ctx context.Context, commit, parent *doltdb.Commit) (rebaseRoot *doltdb.RootValue, err error)

// AsyncReplayAllBranches rewrites the history of all branches in the repo using the |replay| function.
func AsyncReplayAllBranches(ctx context.Context, dEnv *env.DoltEnv, replay AsyncReplayCommitFn, nerf NeedsRebaseFn) error {
	branches, err := dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return err
	}

	return asyncRebaseRefs(ctx, dEnv, replay, nerf, branches...)
}

// AsyncReplayCurrentBranch rewrites the history of the current branch using the |replay| function.
func AsyncReplayCurrentBranch(ctx context.Context, dEnv *env.DoltEnv, replay AsyncReplayCommitFn, nerf NeedsRebaseFn) error {
	return asyncRebaseRefs(ctx, dEnv, replay, nerf, dEnv.RepoState.CWBHeadRef())
}

func asyncRebaseRefs(ctx context.Context, dEnv *env.DoltEnv, replay AsyncReplayCommitFn, nerf NeedsRebaseFn, refs ...ref.DoltRef) error {
	ddb := dEnv.DoltDB
	cwbRef := dEnv.RepoState.CWBHeadRef()
	dd, err := dEnv.GetAllValidDocDetails()
	if err != nil {
		return err
	}

	heads := make([]*doltdb.Commit, len(refs))
	for i, dRef := range refs {
		heads[i], err = ddb.ResolveRef(ctx, dRef)
		if err != nil {
			return err
		}
	}

	newHeads, err := asynceRebase(ctx, ddb, replay, nerf, heads...)
	if err != nil {
		return err
	}

	for i, dRef := range refs {

		switch dRef.(type) {
		case ref.BranchRef:
			err = ddb.DeleteBranch(ctx, dRef)
			if err != nil {
				return err
			}
			err = ddb.NewBranchAtCommit(ctx, dRef, newHeads[i])

		default:
			return fmt.Errorf("cannot rebase ref: %s", ref.String(dRef))
		}

		if err != nil {
			return err
		}
	}

	cm, err := dEnv.DoltDB.ResolveRef(ctx, cwbRef)
	if err != nil {
		return err
	}

	r, err := cm.GetRootValue()
	if err != nil {
		return err
	}

	_, err = dEnv.UpdateStagedRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingRoot(ctx, r)
	if err != nil {
		return err
	}

	err = dEnv.PutDocsToWorking(ctx, dd)
	if err != nil {
		return err
	}

	_, err = dEnv.PutDocsToStaged(ctx, dd)
	return err
}

func asynceRebase(ctx context.Context, ddb *doltdb.DoltDB, replay AsyncReplayCommitFn, nerf NeedsRebaseFn, origins ...*doltdb.Commit) ([]*doltdb.Commit, error) {
	var rebasedCommits []*doltdb.Commit
	vs := make(visitedSet)
	eg, ctx := errgroup.WithContext(ctx)

	for _, cm := range origins {
		rc, err := asyncRebaseRecursive(ctx, eg, ddb, replay, nerf, vs, cm)

		if err != nil {
			return nil, err
		}

		rebasedCommits = append(rebasedCommits, rc)
	}

	return rebasedCommits, nil
}

func asyncRebaseRecursive(ctx context.Context, eg *errgroup.Group, ddb *doltdb.DoltDB, replay AsyncReplayCommitFn, nerf NeedsRebaseFn, vs visitedSet, commit *doltdb.Commit) (*doltdb.Commit, error) {
	commitHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	visitedCommit, found := vs[commitHash]
	if found {
		// base case: reached previously rebased node
		return visitedCommit, nil
	}

	needToRebase, err := nerf(ctx, commit)
	if err != nil {
		return nil, err
	}
	if !needToRebase {
		// base case: reached bottom of DFS,
		return commit, nil
	}

	allParents, err := ddb.ResolveAllParents(ctx, commit)
	if len(allParents) < 1 {
		return nil, fmt.Errorf("commit: %s has no parents", commitHash.String())
	}

	// start replay operations concurrently on our way down
	var rebasedRoot *doltdb.RootValue
	eg.Go(func() error {
		rebasedRoot, err = replay(ctx, commit, allParents[0])
		return err
	})

	var allRebasedParents []*doltdb.Commit
	for _, p := range allParents {
		rp, err := asyncRebaseRecursive(ctx, eg, ddb, replay, nerf, vs, p)

		if err != nil {
			return nil, err
		}

		allRebasedParents = append(allRebasedParents, rp)
	}

	if err = eg.Wait(); err != nil {
		return nil, err
	}

	valueHash, err := ddb.WriteRootValue(ctx, rebasedRoot)
	if err != nil {
		return nil, err
	}

	oldMeta, err := commit.GetCommitMeta()
	if err != nil {
		return nil, err
	}

	rebasedCommit, err := ddb.CommitDanglingWithParentCommits(ctx, valueHash, allRebasedParents, oldMeta)
	if err != nil {
		return nil, err
	}

	vs[commitHash] = rebasedCommit
	return rebasedCommit, nil
}
