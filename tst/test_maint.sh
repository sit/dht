#!/bin/sh

#
# Overall plan:
# 1. Create a configuration file.
# 2. Start one dhash process.
# 3. Wait for stabilization.  Use dbm to insert a bunch of blocks.
# 4. Wait for maintenance.  Check that efrags copies of objects
#    appear everywhere.
# 5. Start a second dhash process.
# 6. wait for maintenance.  Check that some data is copied to new vnodes.
#    Make sure we can still read everything with dbm.
# 7. Kill first dhash.  Check that any left-over data is re-replicated.
# 8. Repeat for various efrag/dfrag/nsucc combos, and PT and Carbonite.
# 9. Repeat for NOAUTH blocks too.
#

# Defaults
NREPLICAS=2
CTYPE=chash
CEXT=c

batchsize=1000
lifetime=0

# Well known node
WKN=127.0.0.1
WKP=3250

# Paths
prefix=maint
TMPDIR=/tmp/$prefix
CONF=$TMPDIR/test_maint.conf

# Binaries
START_DHASH=lsd/start-dhash
DBM=tools/dbm
WALK=tools/walk
DBDUMP=tools/dbdump
if [ -f Makefile ];
then
    eval $(egrep '^top_(srcdir|builddir) =' Makefile | sed 's/ //g')
    START_DHASH=$top_srcdir/lsd/start-dhash
    DBM=$top_builddir/tools/dbm
    DBDUMP=$top_builddir/tools/dbdump
fi

# {{{ create_conf nreplicas
create_conf () # nreplicas
{
    nreplicas=$1
    cat <<EOF > $CONF
chord.rpc_mode tcp
dhash.dfrags 1
dhash.efrags $nreplicas
dhash.replica $nreplicas
dhash.repair_timer 60
EOF
}
# }}}
# {{{ start_dhash name mode port nvnodes
start_dhash () # name mode port nvnode
{
    name=$1
    mode=$2
    port=$3
    nvnode=$4
    shift 4
    $START_DHASH --root ${prefix}-$name --maintmode $mode -m pnsrec \
        -j $WKN:$WKP -l 127.0.0.1 -p $port -v $nvnode \
	-O $CONF "$@"
}
# }}}
# {{{ wait_stable
wait_stable ()
{
    ready=no
    sleep 5
    nvnodes=0
    for d in $prefix-*
    do
	thisvnodes=$(fgrep 'new vnode' $d/log.lsd | wc -l | awk '{ print $1 }')
	if [ $? -eq 0 -a -e $d/dhash-sock ];
	then
	    nvnodes=$(expr $nvnodes + $thisvnodes)
	fi
    done
    echo -n "Expecting $nvnodes ..."
    for i in 1 2 3 4 5 6 7 8 9 10
    do
	echo -n "."
	if $WALK -v -j $WKN:$WKP > $TMPDIR/walk.out
	then
	    # Check that the right number of vnodes is present
	    seenvnodes=$(wc -l $TMPDIR/walk.out | awk '{ print $1 }')
	    if [ $nvnodes -eq $seenvnodes ];
	    then
		ready=yes
		break
	    fi
	fi
	sleep 5
    done
    test $ready = yes
}
# }}}
# {{{ store/fetch count size
TOTALOBJECTS=0
store () {
    count=$1
    size=$2
    shift 2
    TOTALOBJECTS=$(expr $TOTALOBJECTS + $count)
    $DBM ${prefix}-a/dhash-sock store $CTYPE $count $size "$@" > $TMPDIR/store.out 2>&1
}
fetch () {
    count=$1
    size=$2
    shift 2
    $DBM ${prefix}-a/dhash-sock fetch $CTYPE $count $size "$@" > $TMPDIR/fetch.out 2>&1
}
# }}}
# {{{ check_counts nreplicas
check_counts () # nreplicas
{
    nreplicas=$1
    exact=$2
    shift 2
    # Need at least $nreplica copies of $TOTALOBJECTS objects
    for db in ${prefix}-*/db/*.$CEXT
    do
	if [ -e $(dirname $db)/../dhash-sock ]; then
	    $DBDUMP $db | awk '/^key/ { print $2 }'
	fi
    done | sort | uniq -c > $TMPDIR/counts
    if [ -z "$exact" ];
    then
	perl -lane '$n++;
	if ($F[0] < '$nreplicas') {
	    warn "$_ insufficient replicas $F[0]\n";
	    $bad++;
	}
	END {
	  die "wrong num objects $n != '$TOTALOBJECTS'\n"
	    unless $n == '$TOTALOBJECTS';
	  die "$bad objects with insufficient replicas\n" if $bad;
	}' $TMPDIR/counts
    else
	perl -lane '$n++;
	if ($F[0] != '$nreplicas') {
	    warn "$_ wrong number of replicas\n";
	    $bad++;
	}
	END {
	  die "wrong num objects $n != '$TOTALOBJECTS'\n"
	    unless $n == '$TOTALOBJECTS';
	  die "$bad objects with wrong number replicas\n" if $bad;
	}' $TMPDIR/counts
    fi
}
# }}}
# {{{ expect_repairs
expect_repairs ()
{
    ok=false
    for d in $prefix-*
    do
	# Only consider live nodes.
	# NB This will only catch clean exits.
	if [ ! -e $d/dhash-sock ]; then continue; fi
	# Snapshot the log file so subsequent checks only examine
	# new repairs (or not)
	touch $d/log.lsd.expect
	if diff $d/log.lsd.expect $d/log.lsd | grep repair_add > /dev/null
	then
	    ok=true
	fi
	rsync -aq $d/log.lsd $d/log.lsd.expect
    done
    test $ok = "true"
}
# }}}
# {{{ expect_norepairs
expect_norepairs ()
{
    ok=true
    for d in $prefix-*
    do
	# Only consider live nodes.
	# NB This will only catch clean exits.
	if [ ! -e $d/dhash-sock ]; then continue; fi
	touch $d/log.lsd.expect
	if diff $d/log.lsd.expect $d/log.lsd | grep repair_add > /dev/null
	then
	    echo "  Undesired repairs found for $d"
	    ok=false
	fi
	rsync -aq $d/log.lsd $d/log.lsd.expect
    done
    test $ok = "true"
}
# }}}
# {{{ teardown
teardown ()
{
    echo "Teardown in progress:"
    for d in ${prefix}-*
    do
	if [ -e "$d/dhash-sock" ]
	then
	    echo -n "  Shutting down $d..."
	    kill $(cat $d/pid.adbd) && echo -n "signal sent."
	    while [ -e "$d/dhash-sock" ];
	    do
		echo -n "."
		sleep 1
	    done
	    echo "done."
	fi
    done
    echo "Complete."
}
# }}}
# {{{ cleanup
cleanup ()
{
    teardown
    echo "Cleanup in progress:"
    for d in ${prefix}-*
    do
	if [ -d "$d" ]; then
	    echo -n "  Cleaning $d...";
	    rm -rf $d && echo "done." || echo "failed."
	fi
    done
    echo "Complete."
}
# }}}
# {{{ fail msg
fail () {
    if [ ! -z "$1" ]; then
	echo "FAIL $@"
    else
	echo "FAIL"
    fi
    teardown
    exit 1
}
# }}}

trap fail 1 2 15

mode=${1:-passingtone}

mkdir -p $TMPDIR || exit 1
cleanup

create_conf $NREPLICAS

echo "Starting initial nodes..."
start_dhash a $mode 3250 2 & 
sleep 2
start_dhash b $mode 3246 2 &
sleep 2

echo -n "Waiting for stable..."
wait_stable && echo "OK" || fail "Not stable!"

echo -n "Storing $batchsize objects: "
store $batchsize 1024 seed=1 lifetime=1y && echo "OK" || fail 
echo -n "Fetching $batchsize objects: "
fetch $batchsize 1024 seed=1 && echo "OK" || fail

sleep 5

echo -n "Checking count: "
check_counts $NREPLICAS exact && echo "OK" || fail

echo "Waiting and checking again after repair timers..."
sleep 120 # XXX wait longer?
expect_norepairs || fail
check_counts $NREPLICAS exact && echo "OK" || fail

sleep 5

# Node addition
echo  "Adding third node..."
start_dhash c $mode 3258 2 &
sleep 2
echo -n "Waiting..."
wait_stable && echo "OK" || fail "Not stable!"

echo "Waiting and checking after repair timers..."
sleep 120 # XXX wait longer?
if [ $mode = "carbonite" ];
then
    expect_norepairs || fail
    check_counts $NREPLICAS exact && echo "OK"
else
    expect_repairs || fail
    check_counts $NREPLICAS "" && echo "OK"
fi

sleep 5

echo -n "Inserting $batchsize additional objects: "
store $batchsize 1024 seed=2 lifetime=1y && echo "OK" || fail 
echo -n "Fetching all $TOTALOBJECTS objects: "
fetch $batchsize 1024 seed=1 && \
    fetch $batchsize 1024 seed=2 && echo "OK" || fail

sleep 5

kill `cat $prefix-b/pid.adbd`

echo -n "Waiting for stable after kill..."
wait_stable && echo "OK" || fail

echo "Waiting and checking after repair timers..."
sleep 240
check_counts $NREPLICAS "" && echo "OK" || echo "Blocks lost!"
expect_repairs || fail

echo -n "Inserting $batchsize additional objects: "
store $batchsize 1024 seed=3 lifetime=1y && echo "OK" || fail 
echo -n "Fetching all $TOTALOBJECTS objects: "
fetch $batchsize 1024 seed=1 && \
    fetch $batchsize 1024 seed=2 && \
    fetch $batchsize 1024 seed=3 && echo "OK" || fail

echo  "Re-adding second node..."
start_dhash b $mode 3246 2 &
sleep 2

echo "Waiting and checking after repair timers..."
sleep 180
if [ $mode = "carbonite" ];
then
    expect_norepairs || fail
    check_counts $NREPLICAS exact && echo "OK"
else
    expect_repairs || fail
    check_counts $NREPLICAS "" && echo "OK"
fi

teardown

### vim: foldmethod=marker
