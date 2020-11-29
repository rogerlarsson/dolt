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

package diff

import (
	"context"
	textdiff "github.com/andreyvit/diff"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/fatih/color"
	"io"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)


type DocDiffType int

const (
	AddedDoc DocDiffType = iota
	ModifiedDoc
	RemovedDoc
)

type DocDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	DocToType   map[string]DocDiffType
	Docs        []string
}


// NewDocDiffs returns DocDiffs for Dolt Docs between two roots.
func NewDocDiffs(ctx context.Context, from, to *doltdb.RootValue) (*DocDiffs, error) {
	diffs := DocDiffs{
		DocToType: make(map[string]DocDiffType),
	}

	deltas, err := GetDocDeltas(ctx, from, to)
	if err != nil {
		return nil, err
	}

	for _, dd := range deltas {
		switch {
		case dd.IsAdd():
			diffs.DocToType[dd.Name] = AddedDoc
			diffs.NumAdded++
		case dd.IsDrop():
			diffs.DocToType[dd.Name] = RemovedDoc
			diffs.NumRemoved++
		case dd.IsModify():
			diffs.DocToType[dd.Name] = ModifiedDoc
			diffs.NumModified++
		}
		diffs.Docs = append(diffs.Docs, dd.Name)
	}
	sort.Strings(diffs.Docs)

	return &diffs, nil
}

// Len returns the number of docs in a DocDiffs
func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

// GetDocDiffs retrieves staged and unstaged DocDiffs.
func GetDocDiffs(ctx context.Context, dEnv *env.DoltEnv) (*DocDiffs, *DocDiffs, error) {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	stagedDocDiffs, err := NewDocDiffs(ctx, headRoot, stagedRoot)
	if err != nil {
		return nil, nil, err
	}

	notStagedDocDiffs, err := NewDocDiffs(ctx, stagedRoot, workingRoot)
	if err != nil {
		return nil, nil, err
	}

	return stagedDocDiffs, notStagedDocDiffs, nil
}

type DocDelta struct {
	// doc names don't change
	Name string

	FromText *string
	ToText   *string
}

func (dd DocDelta) IsDrop() bool {
	return dd.FromText != nil && dd.ToText == nil
}

func (dd DocDelta) IsAdd() bool {
	return dd.FromText == nil && dd.ToText != nil
}

func (dd DocDelta) IsModify() bool {
	return dd.FromText != nil && dd.ToText != nil
}

func GetDocDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []DocDelta, err error) {
	fromDocs, err := doltdb.ReadDocs(ctx, fromRoot)
	if err != nil {
		return nil, err
	}

	toDocs, err := doltdb.ReadDocs(ctx, toRoot)
	if err != nil {
		return nil, err
	}

	for name, ft := range fromDocs {
		var toText *string
		tt, ok := toDocs[name]
		if ok {
			toText = &tt
		}

		deltas = append(deltas, DocDelta{
			Name:     name,
			FromText: &ft,
			ToText:   toText,
		})

		delete(toDocs, name) // consume doc Name
	}

	for name, tt := range toDocs {
		deltas = append(deltas, DocDelta{
			Name:   name,
			ToText: &tt,
		})
	}

	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].Name < deltas[j].Name
	})

	return deltas, err
}

func DiffDoltDocs(ctx context.Context, wr io.WriteCloser, from, to *doltdb.RootValue, docs *set.StrSet) error {
	deltas, err := GetDocDeltas(ctx, from, to)
	if err != nil {
		return err
	}

	bold := color.New(color.Bold)

	for _, dd := range deltas {
		if !docs.Contains(dd.Name) {
			continue
		}

		if dd.FromText == nil {
			err = printAddedDoc(wr, bold, dd)
		} else if dd.ToText == nil {
			err = printDeletedDoc(wr, bold, dd)
		} else {
			err = printModifiedDoc(wr, bold, dd)
		}
		if err != nil {
			break
		}
	}

	return err
}

func printAddedDoc(wr io.WriteCloser, bold *color.Color, dd DocDelta) (err error) {
	return iohelp.WriteLines(wr,
		bold.Sprintf("diff --dolt a/%[1]s b/%[1]s", dd.Name),
		bold.Sprint("added doc"))
}

func printDeletedDoc(wr io.WriteCloser, bold *color.Color, dd DocDelta) (err error) {
	return iohelp.WriteLines(wr,
		bold.Sprintf("diff --dolt a/%[1]s b/%[1]s", dd.Name),
		bold.Sprintf("deleted doc"))
}

func printModifiedDoc(wr io.WriteCloser, bold *color.Color, dd DocDelta) (err error) {
	err = iohelp.WriteLines(wr,
		bold.Sprintf("diff --dolt a/%[1]s b/%[1]s", dd.Name),
		bold.Sprintf("--- a/%s", dd.Name),
		bold.Sprintf("+++ b/%s", dd.Name))
	if err != nil {
		return err
	}

	lines := textdiff.LineDiffAsLines(*dd.FromText, *dd.ToText)

	for _, line := range lines {
		switch string(line[0]) {
		case"+":
			err = iohelp.WriteLine(wr, color.GreenString("+ " + line[1:]))
		case "-":
			err = iohelp.WriteLine(wr, color.RedString("- " + line[1:]))
		default:
			err = iohelp.WriteLine(wr, " " + line)
		}
		if err != nil {
			break
		}
	}
	return err
}
