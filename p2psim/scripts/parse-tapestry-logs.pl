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

    my %lookups = ();
    my $hash;
    my @stat_vals = ();
    my @stat_nums = ();
    while(<LOG>) {
	if( /(\d+): \((\d+)\/\d+\).*Lookup for key (\d+)$/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    $hash = {};
	    $lookups{"$ip-$key"} = $hash;
	    $hash->{"starttime"} = $t;
	    $hash->{"failures"} = 0;
	} elsif( /\d+: \(\d+\/\d+\).*Failure happened .* key (\d+), .* for node (\d+)$/ ) {
	    my $key = $1;
	    my $ip = $2;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    $ht->{"failures"}++;
	} elsif( /(\d+): \((\d+)\/\d+\).*Lookup failed for key (\d+)$/ ) {
	    my $t = $1;
	    my $ip = $2;
	    my $key = $3;
	    my $ht = $lookups{"$ip-$key"};
	    if( !$ht ) {
		die( "no lookup found for $ip, $key" );
	    }
	    &print_stat( DAT, $1-($ht->{"starttime"}), $ip, $key, 0, 0, -1,
			 $ht->{"failures"}, "NONE", "NONE", "NONE" );
	    delete $lookups{"$ip-$key"};
	} elsif( /(\d+): \((\d+)\/\d+\).*Lookup complete for key (\d+): ip (\d+), id (\d+), hops (\d+)$/ ) {
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
			 $ht->{"failures"}, $oip, $owner, "NONE" );
	    delete $lookups{"$ip-$key"};
	} elsif( /(\d+): \((\d+)\/\d+\).*Lookup incorrect for key (\d+): ip (\d+), id (\d+), real root (\d+) hops (\d+)$/ ) {
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
	    &print_stat( DAT, $1-($ht->{"starttime"}), $ip, $key, 1, 1, $hops,
			 $ht->{"failures"}, $oip, $owner, $realroot );
	    delete $lookups{"$ip-$key"};
	} elsif( /STATS: (.*)$/ ) {

	    my @statar = split( /\s+/, $1 );
	    for( my $i = 0; $i <= $#stats; $i++ ) {
		if( $statar[$i*2] ne $stats[$i] ) {
		    die( "stat " . $statar[$i*2] . " doesn't match" );
		}
		$stat_vals[$i] += $statar[$i*2+1];
		$stat_nums[$i]++;
	    }

	}

    }

    for( my $i = 0; $i <= $#stats; $i++ ) {
	print DAT "$stats[$i] $stat_vals[$i] $stat_nums[$i]\n";
    }

    close( DAT );
    close( LOG );

}

sub print_stat {

    my ($fd, $time, $ip, $key, $complete, $correct, $hops, $failures, 
	$oip, $owner, $real_owner ) = @_;

    print $fd "$time $ip $key $complete $correct $hops $failures " . 
	"$oip $owner $real_owner\n";

}
