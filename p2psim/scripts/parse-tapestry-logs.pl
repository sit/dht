#!/usr/bin/perl -w
#use strict;

# gather statistics from each log and put it into dat format

my @logs = @ARGV;

my @stats = qw( join lookup nodelist mc ping backpointer mcnotify nn repair );

foreach my $log (@logs) {

    if( !( -f $log ) ) {
	print STDERR "$log is not a file, skipping.\n";
	next;
    }

    open( LOG, "<$log" ) or die( "Couldn't open $log" );
    $log =~ s/log/dat/;
    open( DAT, ">$log" ) or die( "Couldn't open $log" );

    my $oldfd = select(DAT);
    $| = 1;
    my %lookups = ();
    my %joins = ();
    my %joined = ();
    my $hash;
    my @stat_vals = ();
    my @stat_nums = ();
    while(<LOG>) {
	if( /(\d+): \((\d+)\/[\w\-]+\).*Lookup for key ([\w\-]+)$/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    $hash = {};
	    $lookups{"$ip-$key"} = $hash;
	    $hash->{"starttime"} = $t;
	    $hash->{"failures"} = 0;
	} elsif( /\d+: \(\d+\/[\w\-]+\).*Failure happened .* key ([\w\-]+), .* for node (\d+)$/ ) {
	    my $key = $1;
	    my $ip = $2;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    $ht->{"failures"}++;
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Lookup failed for key ([\w\-]+)$/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    &print_stat( DAT, $1-($ht->{"starttime"}), $ip, $key, 0, 0, -1,
			 $ht->{"failures"}, "NONE", "NONE", "NONE", 
			 $ht->{"starttime"} );
	    delete $lookups{"$ip-$key"};
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Lookup complete for key ([\w\-]+): ip (\d+), id ([\w\-]+), hops (\d+)/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    my $oip = $4;
	    my $owner = $5;
	    my $hops = $6;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    &print_stat( DAT, $1-($ht->{"starttime"}), $ip, $key, 1, 1, $hops,
			 $ht->{"failures"}, $oip, $owner, "NONE",
			 $ht->{"starttime"});
	    delete $lookups{"$ip-$key"};
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Lookup incorrect for key ([\w\-]+): ip (\d+), id ([\w\-]+), real root ([\w\-]+) hops (\d+)/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    my $oip = $4;
	    my $owner = $5;
	    my $realroot = $6;
	    my $hops = $7;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    &print_stat( DAT, $1-($ht->{"starttime"}), $ip, $key, 1, 0, $hops,
			 $ht->{"failures"}, $oip, $owner, $realroot,
			 $ht->{"starttime"});
	    delete $lookups{"$ip-$key"};
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Tapestry join/ ) {
	    my $t = $1;
	    my $ip = $2;
	    $joins{$ip} = $t;
	    print "joinstart $ip $t\n";
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Finishing joining/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $time = 0;
	    $joined{$ip} = $t;
	    if( defined $joins{$ip} ) {
		$time = $t - $joins{$ip};
	    }
	    print "join $ip $t $time\n";
	} elsif( /(\d+): \((\d+)\/[\w\-]+\).*Tapestry crash/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $was_joined = 0;
	    if( defined $joined{$ip} and $joined{$ip} != 0 ) {
		$was_joined = 1;
	    }
	    $joined{$ip} = 0;
	    print "crash $ip $t $was_joined\n";
	} elsif( /STATS: (.*)$/ ) {

	    my @statar = split( /\s+/, $1 );
	    for( my $i = 0; $i <= $#stats; $i++ ) {
		if( $statar[$i*3] ne $stats[$i] ) {
		    die( "stat " . $statar[$i*2] . " doesn't match" );
		}
		$stat_vals[$i] += $statar[$i*3+1];
		$stat_nums[$i] += $statar[$i*3+2];
	    }

	} elsif( /^(\d): (.*)$/ ) {
	    print STDERR $_;
	    my $tot = 0;
	    my $c = 0;
	    my $index = $1;
	    my @entries = split( /\s+/, $2 );
	    foreach my $entry (@entries) {
		my @ind = split( /\//, $entry );
		if( $#ind < 2 or $ind[2] == 0 ) {
		    next;
		}
		$tot += $ind[2];
		if( $ind[2] > 50000 ) {
		    print STDERR "#$index $ind[2]!\n";
		}
		$c++;
	    }

	    if( $c ) {
		print "$index: average rtt=" . ($tot/$c) . "\n";
	    }

	} elsif( /lookup/ or /average/ or /rate/ ) {
	    print $_;
	} else {

	}

    }
    for( my $i = 0; $i <= $#stats; $i++ ) {
	print "$stats[$i] $stat_vals[$i] $stat_nums[$i]\n";
    }

    select( $oldfd );
    close( DAT ) or die( "Couldn't close $log!" );
    close( LOG );

}

sub print_stat {

    my ($fd, $time, $ip, $key, $complete, $correct, $hops, $failures, 
	$oip, $owner, $real_owner, $starttime ) = @_;

    print "$time $ip $key $complete $correct $hops $failures " . 
	"$oip $owner $real_owner $starttime\n";

}
