#!/usr/local/bin/perl

# tuneable parameters
$blocks_init = 10;
$blocks_inc = 10;
$blocks_end = 10;
$size_init = 8192;
$size_inc = 2;
$size_end = 262144;
$sleeptime = 300;

$reps = 5;
$count = 30000;

print "size #blocks storetime fetchtime\n";

$i = $reps;
while ($i > 0) {

    $blocks = $blocks_init;
    while ($blocks <= $blocks_end) {

	$size = $size_init;
	while ($size <= $size_end) {

	    $out = `./dbm 0 ../lsd/csock2 $blocks $size - s 100 $count`;
	    if ($out =~ /Total Elapsed\: (.+)/) {
		$storetime = $1;
	    } else {
		print $out;
		die "can't find elapsed time\n";
	    }
	    sleep $sleeptime;

	    $out = `./dbm 0 ../lsd/csock2 $blocks $size - f 100 $count`;
	    if ($out =~ /Total Elapsed\: (.+)/) {
		$fetchtime = $1;
	    } else {
		print $out;
		die "can't find elapsed time\n";
	    }
	    sleep $sleeptime;

	    $count++;
	    print "$size $blocks $storetime $fetchtime\n";
	
	    $size *= $size_inc;
	}
	$blocks *= $blocks_inc;
    }
    $i--;
}
