#!/usr/bin/perl -w
use strict;

# collapse each log into one data point, print to STDOUT

my @logs = @ARGV;

#my @stats = qw( ping repair );
my @stats = qw( join nodelist mc ping backpointer mcnotify nn repair );

my $FAILURE_PENALTY = 2;
my $COMPLETE_FAILURE_PENALTY = 20;
my $INCORRECT_FAILURE_PENALTY = 10;

foreach my $log (@logs) {

    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );

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

    my $total_hops = 0;
    my $num_lookups = 0;
    my $total_msgs = 0;
    while(<LOG>) {
	if( /(\d+) \d+ \w+ (\d) (\d) -?(\d+) (\d+) .+ .+ .+/ ) {
	    my $time = $1;
	    my $complete = $2;
	    my $correct = $3;
	    my $hops = $4;
	    my $failures = $5;

	    if( $complete eq "1" ) {
		#$total_hops += $hops + $failures*$FAILURE_PENALTY;
		$total_hops += $time;
		$num_lookups++;
#		if( $correct ne "1" ) {
#		    $total_hops += $INCORRECT_FAILURE_PENALTY;
#		}
	    } else {
#		$total_hops += $COMPLETE_FAILURE_PENALTY;
		$total_hops += $time;
		$num_lookups++;
	    }

	} elsif( /(.+) (\d+) \d+$/ ) {

	    my $stat = $1;
	    my $msgs = $2;

	    if( grep( /$stat/, @stats ) ) {
		$total_msgs += $msgs;
	    }

	} else {
	    die( "unrecognized line: $_" );
	}

    }

#    print STDERR "$log\n";
    my $av_hop = $total_hops/$num_lookups;
    print "\# $base $redun $rln $stabtimer:\n";
    print "$total_msgs $av_hop\n";

    close( LOG );

}

