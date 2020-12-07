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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	children []sql.Expression
}

// TODO Refactor from commands/mergo.go
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
	panic("implement me")
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

	ddb, ok := dSess.GetDoltDB(dbName)

	if !ok {
		return nil, fmt.Errorf("Could not load %s", dbName)
	}

	rsr, ok := dSess.GetDoltDBRepoStateReader(dbName)

	if !ok {
		return nil, fmt.Errorf("Could not load the %s RepoStateReader", dbName)
	}

	rsw, ok := dSess.GetDoltDBRepoStateWriter(dbName)

	if !ok {
		return nil, fmt.Errorf("Could not load the %s RepoStateWriter", dbName)
	}

	ap := createArgParser()

	// TODO: This can get refactored Get the args for DOLT_MERGE
	args := make([]string, len(d.children))
	for i := range d.children {
		childVal, err := d.children[i].Eval(ctx, row)

		if err != nil {
			return nil, err
		}

		text, err := sql.Text.Convert(childVal)

		if err != nil {
			return nil, err
		}

		args[i] = text.(string)
	}

	apr := cli.ParseArgs(ap, args, nil)

	if apr.ContainsAll("squash", "no-ff") {
		return nil, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", "squash", "no-ff")
	}



	return "change this fam", nil
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

