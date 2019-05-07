#!/usr/bin/env bats

setup() { 
    if [ -z "$BATS_TMPDIR" ]; then
        export BATS_TMPDIR=$HOME/batstmp/
        mkdir $BATS_TMPDIR
    fi

    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir remotes-$$
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    mkdir dolt-repo-$$
    cd dolt-repo-$$
    dolt init
    mkdir "dolt-repo-clones"
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo-$$
    pkill -2 remotesrv
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    pgrep remotesrv
}

@test "add a remote using dolt remote" {
    run dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    run dolt remote add test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage:" ]] || false
}

@test "push and pull from a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt push poop master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say Already up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
    run dolt pull poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "rename a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote rename test-remote renamed-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "renamed-remote" ]] || false
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote rename poop test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "remove a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote remove test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote remove poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "clone a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone localhost:50051/test-org/test-repo --insecure
    [ "$status" -eq 0 ]
    # Change this back to not a regex after we remove the extra refs output
    [[ "$output" =~ "cloning localhost:50051/test-org/test-repo" ]] || false
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "clone a non-existant remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    cd "dolt-repo-clones"
    run dolt clone foo/bar
    [ "$status" -eq 1 ]
    skip "Cloning a non-existant repository fails weirdly and leaves trash"
    [ "$output" = "fatal: repository 'foo/bar' does not exist" ]
    [[ ! "$output" =~ "permission denied" ]] || false
    [ ! -d bar ]
}

@test "dolt merge with origin/master syntax." {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    dolt fetch
    run dolt fetch refs/heads/master:refs/remotes/poop/master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote 'poop'" ]] || false
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt log
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test commit" ]] || false
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    run dolt fetch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "dolt fetch and merge with remotes/origin/master syntax" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    skip "This remotes/origin/master syntax no longer works but works on git"
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    dolt fetch origin master
    [ "$status" -eq 0 ]
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
}

@test "try to push a remote that is behind tip" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt push origin master
    [ "$status" -eq 0 ]
    [ "$output" = "Everything up-to-date" ] || false
    dolt fetch
    run dolt push origin master
    skip "dolt push when behind returns a 0 exit code now. should be 1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "generate a merge with no conflict with a remote branch" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test2
    dolt add test2
    dolt commit -m "another test commit"
    run dolt pull origin
    skip "This panics right now with a panic: runtime error: invalid memory address or nil pointer dereference"
    [ "$status" -eq 0 ]
}