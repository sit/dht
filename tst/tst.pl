#!/usr/bin/perl

my $p = $ARGV[0];
my $ip = $ARGV[1];
my $port = $ARGV[2];
my $v = $ARGV[3];
my $n = $ARGV[4];
my $o = $ARGV[5];

print "tst: $v $n\n";

sub spawn {
    my $pid;

    print "@_\n";

    if ($pid = fork) {
	return $pid;
    } elsif (defined ($pid)) {
 	system(@_);
	die "child is done @_\n";
    } else { 
  	die "cannot fork\n";
    }
}

spawn ("$p -d /var/tmp/db-0 -j $ip:$port -l $ip  -p $port -S /var/tmp/sock0 -v $v 2> $o/out0");

foreach $i (1 .. $n-1) {
    sleep 5;
    spawn ("$p -d /var/tmp/db-$i -j $ip:$port -l $ip -v $v 2> $o/out$i");
}

sleep 1;

print "forked daemons: $n\n";
