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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

const DoltAddFuncName = "dolt_add"

const (
	allParam = "all"
)

type DoltAddFunc struct {
	children []sql.Expression
}

func (d DoltAddFunc) Resolved() bool {
	return true
}

func (d DoltAddFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for _, child := range d.children {
		childrenStrings = append(childrenStrings, child.String())
	}
	return fmt.Sprintf("DOLT_ADD(%s)", strings.Join(childrenStrings, " "))
}

func (d DoltAddFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltAddFunc) IsNullable() bool {
	return false
}

func (d DoltAddFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	// Get the args for DOLT_ADD.
	args := make([]string, 1)
	for i := range d.children {
		temp := d.children[i].String()
		str := trimQuotes(temp)
		args = append(args, str)
	}

	apr := cli.ParseArgs(ap, args, nil)

	if apr.ContainsArg(doltdb.DocTableName) {
		// Only allow adding the dolt_docs table if it has a conflict to resolve
		hasConflicts, _ := actions.DocCnfsOnWorkingRoot(ctx, ddb, rsr)
		if !hasConflicts {
			return "", nil
		}
	}

	allFlag := apr.Contains(allParam)

	var err error
	if apr.NArg() == 0 && !allFlag {
		cli.Println("Nothing specified, nothing added.\n Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		err = actions.StageAllTables(ctx, ddb, rsr, rsw)
	} else {
		err = actions.StageTables(ctx, ddb, rsr, rsw, apr.Args())
	}

	return "Files added", err
}

func (d DoltAddFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltAddFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltAddFunc(children...)
}

// NewDoltCommitFunc creates a new DoltCommitFunc expression.
func NewDoltAddFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltAddFunc{children: args}, nil
}
