#!/usr/bin/perl -w
use strict;

# collapse each log into one data point, print to STDOUT

my @logs = @ARGV;
my %starts = ();
my $bucket_factor = 5000;

foreach my $log (@logs) {

    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    my $base = 0; 
    my $redun = 0;
    my $rln = 0;
    my $stabtimer = 0;
    if( $log =~ /-(\d+)-(\d+)-(\d+)-(\d+).dat$/ ) {
	$base = $1;
	$redun = $2;
	$rln = $3;
	$stabtimer = $4;
    } else {
	print STDERR "bad log name $log\n";
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );

    my %correct_lat = ();
    my %incorrect_lat = ();
    my %failed_lat = ();
    my %lives = ();
    my %joinstarts = ();
    while(<LOG>) {
	if( /(\d+) \d+ \w+ (\d) (\d) -?(\d+) (\d+) .+ .+ .+ (\d+)/ ) {
	    my $time = $1;
	    my $complete = $2;
	    my $correct = $3;
	    my $hops = $4;
	    my $failures = $5;
	    my $start = $6;

	    my $lats;
	    my $key = int( $start/$bucket_factor );
	    $starts{$key} = 1;
	    if( $complete eq "1" ) {
		if( $correct eq "1" ) {
		    if( !defined $correct_lat{ $key } ) {
			$correct_lat{ $key } = "";
		    }
		    $correct_lat{ $key } .= "$time ";
		} else {
		    if( !defined $incorrect_lat{ $key } ) {
			$incorrect_lat{ $key } = "";
		    }
		    $incorrect_lat{ $key } .= "$time ";
		}
	    } else {
		if( !defined $failed_lat{ $key } ) {
		    $failed_lat{ $key } = "";
		}
		$failed_lat{ $key } .= "$time ";
	    }

	} elsif( /^joinstart (\d+) (\d+)/ ) {
	    $joinstarts{$1} = $2;
	} elsif( /^join (\d+) (\d+)/ ) {
	    my $key = int( $2/$bucket_factor );
	    $starts{$key} = 1;
	    if( !defined $lives{$key} ) {
		$lives{$key} = 0;
	    }
	    $lives{$key}++;
	    $joinstarts{$1} = 0;
	} elsif( /^crash (\d+) (\d+) (\d)/ ) {
	    my $key = int( $2/$bucket_factor );
	    $starts{$key} = 1;
	    if( !defined $lives{$key} ) {
		$lives{$key} = 0;
	    }
	    if( $3 eq "1" ) {
		$lives{$key}--;
	    }
	    $joinstarts{$1} = 0;
	} else {
	    # nothing
	}

    }

    close( LOG );

    foreach my $k (keys(%joinstarts)) {
	if( $joinstarts{$k} ) {
	    print "joinfail: $k " . $joinstarts{$k} . "\n";
	}
    }

    $log =~ s/\.dat/\-rrk\.dat/;
    
    open( LOG, ">$log" ) or die( "Couldn't open $log" );	

    print LOG "# complete% correct% ave_lat count num_live\n"; 

    my $currlive = 0;
    foreach my $t (sort {$a <=> $b} keys(%starts)) {

	# for each key, figure out the completed and correctness lines,
	# as well as average latency
	
	my $corr = $correct_lat{$t};
	if( !defined $corr ) {
	    $corr = "";
	}
	my $incorr = $incorrect_lat{$t};
	if( !defined $incorr ) {
	    $incorr = "";
	}
	my $fail = $failed_lat{$t};
	if( !defined $fail ) {
	    $fail = "";
	}

	my @corrs = split( /\s+/, $corr );
	my @incorrs = split( /\s+/, $incorr );
	my @fails = split( /\s+/, $fail );

	my $total = ($#corrs+1) + ($#incorrs+1) + ($#fails+1);
	my $comper = (($#corrs+1) + ($#incorrs+1))/$total;
	my $corrper = ($#corrs+1)/(($#corrs+1) + ($#incorrs+1));

	my $tot_lat = 0;
	foreach my $l (@corrs) {
	    $tot_lat += $l;
	}
	foreach my $l (@incorrs) {
	    $tot_lat += $l;
	}
	foreach my $l (@fails) {
	    $tot_lat += $l;
	}

	my $avlat = $tot_lat/$total;

	if( defined $lives{$t} ) {
	    $currlive += $lives{$t};
	}

	print LOG "" . ($t*$bucket_factor) . " $comper $corrper $avlat $total $currlive\n";

    }
    
    close( LOG );

    # gnuplot it
    my $gp = "/tmp/rrk$$.gnuplot";
    open( GP, ">$gp" ) or die( "couldn't open $gp" );

    my $eps = $log;
    $eps =~ s/dat/eps/;

    print GP <<End;

set terminal postscript color "Times-Roman" 22
set output "$eps"
set ylabel "Percent"
set y2label "average latency"
set title "Tapestry - 1024 nodes - 5 retries - ($base,$redun,$rln,$stabtimer)"
set xlabel "Time"
set yrange [0:1]
set y2tics
set ytics nomirror
plot "$log" using (\$1/1000):(\$2) t "% Complete" with lines, \\
     "$log" using (\$1/1000):(\$3) t "% Correct" with lines, \\
     "$log" using (\$1/1000):(\$4)  axes x1y2 t "Ave Latency" with lines

End

    close( GP );

    system( "gnuplot $gp; rm -f $gp" ) and die( "Couldn't do it" );

}
