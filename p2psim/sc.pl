#!/usr/local/bin/perl

use strict;

#
# sc.pl <topfile> <eventfile> < scenerio
# 
# takes as input a scenario and generates topology and an event file
#
# syntax of a scenario:
#
# net: <number of nodes>,<topology>,<placement>,<protocol>
#    <placement> ::= linear | random <number> <number>
#
# event: <node id>,<event-type>,<args>
# events: <number of events>,<interval>,<event-type>,<args>

open TOP, ">$ARGV[0]" || die "Could not open $ARGV[0]: $!\n";
open EV, ">$ARGV[1]" || die "Could not open $ARGV[1]: $!\n";

my $line;
my $nnodes;
my $time = 1;

while ($line = <STDIN>) {
    chomp($line);
    if ($line =~/^net: (.*)/) {
	donet (split(/,/ , $1));
    }
    if ($line =~/^event: (.*)/) {
	doevent (split(/,/, $1));
    }
    if ($line =~/^events: (.*)/) {
	doevents (split(/,/, $1));
    }
}

close TOP || die "Could not close $ARGV[0]: $!\n";
close EV || die "Could not close $ARGV[1]: $!\n";

sub donet {
    my ($n, $top, $place,$pro) = @_;
    print "donet: $n $top $place $pro\n";
    $nnodes = $n;
    print TOP "topology $top\n\n";
    for (my $i = 1; $i <= $n; $i++) {
	if ($place =~ /random (\d+) (\d+)/) {
	    my $x = int(rand $1) + 1;
	    my $y = int(rand $2) + 1;
	    print TOP "$i $x,$y $pro\n";
	} elsif ($place == "linear") {
	    print TOP "$i $i,0 $pro\n";
	} else {
	    print STDERR "Unknown placement $place\n";
	    exit (-1);
	}
    }
}

sub doevent {
    my ($n,$type,@args) = @_;
    print "doevent: $n $type @args\n";
    print EV "node $time $n $type @args\n";
}

sub doevents {
    my ($n,$interval,$type,@args) = @_;
    print "doevents: $n $interval $type @args\n";
    for (my $i = 1; $i <= $n; $i++) {
	my $node = int(rand $nnodes) + 1;
	$time = $time + $interval;
	print EV "node $time $node $type @args\n";
    }
}

