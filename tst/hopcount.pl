#!/usr/bin/perl

# script to build a chord ring, wait until stable, insert data, and look it up
# run script from the tst directory:
#   hopcount.pl build-directory ip-address result-directory #node #v-per-node
# script creates files for each node in result-directory; at the end all files 
# are cat-ed together in "dump-#node-#v-per-node for later processing (e.g.,
# to compute the number of hops per lookup).  this requires, of course, that dbm
# prints the numbers of hops for each lookup.

$h = $ARGV[0];
$lsd = "$h/lsd/lsd";
$ip = $ARGV[1];
$port = 8888;
$dbm = "$h/devel/dbm";
$r = $ARGV[2];
$d = 1000;

foreach $n ($ARGV[3]) {
    foreach $v ($ARGV[4]) {
	system ("./tst.pl $lsd $ip $port $v $n $r");

	sleep 30;
	system ("./waitstable.pl $lsd $n $r");

	print "insert\n";
	system ("$dbm 0 /var/tmp/sock0 $d 4 $r/s-$n-$v s 0 2>> $r/s-$n-$v 1");
	print "lookup\n";
	system ("$dbm 0 /var/tmp/sock0 $d 4 $r/f-$n-$v f 0 1");

	foreach $k (0 .. $n-1) {
	    system ("./stat.pl $lsd $k");
	}
	sleep 5;
	system ("./kill.pl $lsd");
	sleep 5;
        system ("sync");
	foreach $k (0 .. $n-1) {
	    $s = "============= NODE $k ============";
	    system ("echo $s >> $r/dump-$n-$v");
	    system ("cat $r/out$k >> $r/dump-$n-$v");
	}

	sleep 2;
    }
}
