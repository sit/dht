#!/usr/bin/perl -w

my @logs = @ARGV;

my $headerdone = 0;
my $doneheader = "";

my @stats = qw( BW_PER_TYPE BW_TOTALS LOOKUP_RATES CORRECT_LOOKUPS 
		INCORRECT_LOOKUPS FAILED_LOOKUPS OVERALL_LOOKUPS 
		TIMEOUTS_PER_LOOKUP );

foreach my $log (@logs) {

    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );
    #print STDERR "$log\n";

    my @header = ("#");
    my @log_stats = ();
    my $instats = 0;

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

			if( !$headerdone ) {
			    push @header, ($#header+1) . 
				")$bigstat:$splitstat[0] "; 
			}
			push @log_stats, $splitstat[1];
		    }
		}
	    }
	} elsif( /\<-----STATS-----\>/ ) {
	    $instats = 1;
	} elsif( $instats and /\<-----ENDSTATS-----\>/ ) {
	    $instats = 0;

	    # this file had complete stats, so print out stuff
	    if( !$headerdone ) {
		print "@header\n";
		$headerdone = 1;
	    }
	    
	    $log =~ s/^.*\/([^\/]+)\.log$/$1/;
	    my @logname = split( /\-/, $log );
	    print "# ";
	    for( my $i = 1; $i <= $#logname; $i++ ) {
		my @s = split( /\./, $logname[$i] );
		print $s[0] . " ";
	    }
	    print "\n";
	    print "@log_stats\n";   
	}
    }

    close( LOG );

}
