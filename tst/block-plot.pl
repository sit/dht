#!/usr/local/bin/perl
# ./block-plot.pl [store|fetch] [10|100] < [inputfile] | gnuplot | gv -

$cursize = 0;
$curblocks = 0;
$stime = 0;
$ftime = 0;

if(@ARGV != 2) {
    die "usage: block-plot.pl choose_fetch_or_store choose_#_blocks\n";
}

$fs = $ARGV[0];
$nblocks = $ARGV[1];

print "set terminal postscript default\n";
print "set title '$fs with $nblocks blocks'\n";
print "set xlabel 'block size'\n";
print "set ylabel 'throughput kB/s'\n";
print "plot '-' with lines\n";

while(<STDIN>) {
    chomp;

    if(/^(.+) (.+) (.+) (.+)/) {
	if(($cursize == $1) &&
	   ($curblocks == $2)) {
	    $stime += $3;
	    $ftime += $4;
	} else {
	    if(($stime != 0) &&
	       ($curblocks == $nblocks)) {
		if($fs eq 'fetch') {
		    $avgtime = $cursize*$curblocks / ($ftime / 1000);
		} else {
		    $avgtime = $cursize*$curblocks / ($stime / 1000);
		}
		print "$cursize $avgtime\n";
	    }
	    $cursize = $1;
	    $curblocks = $2;
	    $stime = $3;
	    $ftime = $4;
	}
    } else {
	print "$_\n";
    }

}

if($fs eq 'fetch') {
    $avgtime = $cursize*$curblocks / ($ftime / 1000);
} else {
    $avgtime = $cursize*$curblocks / ($stime / 1000);
}
print "$cursize $avgtime\n";

print "e\nquit\n";
