#!/usr/bin/perl -w
use strict;

# collapse each log into one data point, print to STDOUT

my $perinc_low = shift(@ARGV);
my $perinc_high = shift(@ARGV);
my @logs = @ARGV;

#my @stats = qw( ping repair );
my @stats = qw( join nodelist mc ping backpointer mcnotify nn repair lookup );

foreach my $log (@logs) {

    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );
    print STDERR "$log\n";

    my $base = 0; 
    my $redun = 0;
    my $rln = 0;
    my $stabtimer = 0;
    if( $log =~ /-(\d+)-(\d+)-(\d+)-(\d+).dat$/ ) {
	$base = $1;
	$redun = $2;
	$rln = $3;
	$stabtimer = $4;
    }

    my $total_time = 0;
    my $total_hops = 0;
    my $total_failures = 0;
    my $num_lookups = 0;
    my $total_msgs = 0;
    my $num_incorrect = 0;
    my @rtlevels;
    my @rtcounts;
    while(<LOG>) {
	if( /(\d+) \d+ [\w\-]+ (\d) (\d) -?(\d+) (\d+) .+ .+ .+/ ) {
	    my $time = $1;
	    my $complete = $2;
	    my $correct = $3;
	    my $hops = $4;
	    my $failures = $5;

	    $total_time += $time;
	    $total_hops += $hops;
	    $total_failures += $failures;
	    $num_lookups++;
	    if( !($complete eq "1" and $correct eq "1") ) {
 		$num_incorrect++;
	    }

	} elsif( /(\d): average rtt=([\d\.]+)/ ) {

	    if( !defined $rtlevels[$1] ) {
		$rtlevels[$1] = 0;
		$rtcounts[$1] = 0;
	    }
	    $rtlevels[$1] += $2;
	    $rtcounts[$1]++;

	} elsif( /(.+) (\d+) \d+$/ ) {

	    my $stat = $1;
	    my $msgs = $2;

	    if( grep( /$stat/, @stats ) ) {
		$total_msgs += $msgs;
	    }

	} elsif( /average lookup latency: ([\d\.]+)$/ ) {
	    $num_lookups = -1;
	    $total_time = $1;
	} elsif( /average hops: ([\d\.]+)$/ ) {
	    $num_lookups = -1;
	    $total_hops = $1;
	} elsif( /success rate: ([\d\.]+)$/ ) {
	    $num_lookups = -1;
	    $num_incorrect = $1;
	} else {
#	    die( "unrecognized line: $_" );
	    next;
	}

    }

    my $av_hop = $total_hops/$num_lookups;
    my $av_time = $total_time/$num_lookups;
    my $av_fail = $total_failures/$num_lookups;
    my $succ_rate = (1-$num_incorrect/$num_lookups);
    if( $num_lookups == -1 ) {
	$av_hop = $total_hops;
	$av_time = $total_time;
	$succ_rate = $num_incorrect;
    }
    # only print it if this is an acceptable incorrectness rate
#    if( ($num_incorrect*100/$num_lookups) > $perinc_low and
#	($num_incorrect*100/$num_lookups) <= $perinc_high ) {
	print "\# ";
	for( my $i = 0; $i <= $#rtlevels; $i++ ) {
	    print "$i:" . ($rtlevels[$i]/$rtcounts[$i]) . " ";
	}
	print "\n\# $base $redun $rln $stabtimer:\n";
	print "$total_msgs $av_time $av_hop " . 
	    " $succ_rate $av_fail $num_lookups\n";
#    }

    close( LOG );

}

