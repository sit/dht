#!/usr/bin/perl -w

use strict;

# check for args file first
my $argsfile;
if( $ARGV[0] eq "--args" ) {
    shift(@ARGV);
    $argsfile = shift(@ARGV);
}
my @logs;
my $logfile;
if ($ARGV[0] eq "--logs") {
    shift(@ARGV);
    $logfile = shift(@ARGV);
    open LOGS,"$logfile" or die "cannot open logs $logfile\n";
    @logs = <LOGS>;
    close LOGS;
}else{
    @logs = @ARGV;
}

my @stats = qw( BW_PER_TYPE BW_TOTALS BW_PERNODE BW_PERNODE_IN BW_SPE1NODE BW_SPE2NODE BW_SPE3NODE BW_SPE1NODE_IN BW_SPE2NODE_IN BW_SPE3NODE_IN LOOKUP_RATES CORRECT_LOOKUPS RTABLE
		INCORRECT_LOOKUPS FAILED_LOOKUPS OVERALL_LOOKUPS 
		TIMEOUTS_PER_LOOKUP SLICELEADER_BW);

my @headers = ();
my %stats = ();
my %stats_used = ();

foreach my $log (@logs) {

    chomp $log;
    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );
    #print STDERR "$log\n";

    my @header = ("#");
    my %allheader;
    my @log_stats = ();
    my $instats = 0;
    my @stats_found = ();

    $log =~ s/^.*\/([^\/]+)\.log$/$1/;
    my @logname = split( /\-/, $log );
    my $h =  "# ";
    for( my $i = 1; $i <= $#logname; $i++ ) {
	my @s = split( /\./, $logname[$i] );
	$h .=  $s[0] . " ";
    }
    print STDERR "SHIT $h\n";

    while( <LOG> ) {

	if ( $instats and /(.*):: (.*)$/) {
	    my $bigstat = $1;
	    if( grep( /$bigstat/, @stats ) ) {
		my @space = split( /\s+/, $2 );
		foreach my $stat (@space) {
		    my @splitstat = split( /\:/, $stat );
		    if( $#splitstat != 1 ) {
			die( "Bad stat in $bigstat:: $stat" );
		    } else {

			push @stats_found, "$bigstat:$splitstat[0]";
			push @log_stats, $splitstat[1];
		    }
		}
	    }
	}elsif (/alive \d+ avg (\d+\.\d+).*longest/) {
	  push @stats_found, "SHORTEST_PATH:mean";
	  push @log_stats, $1;
	} elsif( /mystat (\d+)/ ) {
	    push @stats_found, "MY:stat";
	    push @log_stats, $1;
	} elsif( /\<-----STATS-----\>/ ) {
	    $instats = 1;
	} elsif( $instats and /\<-----ENDSTATS-----\>/ ) {
	    $instats = 0;
	    if (!defined($allheader{$h})) {
	      push @headers, $h;
	      $allheader{$h} = 1;
	      print STDERR "pushing $h\n";
	    }
	    for( my $i = 0; $i <= $#stats_found; $i++ ) {
		my $s = $stats_found[$i];
		$stats_used{$s} = "true";
		$stats{"$h$s"} = $log_stats[$i];
	    }
	}
    }

    close( LOG );

}

# now sort all the stats and print the header
my @final_stats = sort(keys(%stats_used));
print "# ";
for( my $i = 0; $i <= $#final_stats; $i++ ) {
    print "$i)$final_stats[$i] ";
}

if( defined $argsfile ) {

    my $i = 1;
    open( ARGS, "<$argsfile" ) or die( "Couldn't open args file: $argsfile" );
    while( <ARGS> ) {
	
	# skip comments
	if( /^\#/ ) {
	    next;
	}
	
	my @args = split( /\s+/ );
	my $argname = shift(@args);
	
	print "param$i)$argname ";
	$i++;
    }

    close( ARGS );

}

print "\n";
foreach my $h (@headers) {
    
    print "$h\n";
    foreach my $s (@final_stats) {
	if( defined $stats{"$h$s"} ) {
	    print $stats{"$h$s"} . " ";
	} else {
	    print "0 ";
	}
    }
    print "\n";

}
