#!/usr//bin/perl

my $p = $ARGV[0];
my $n = $ARGV[1];
my $r = $ARGV[2];
my $s = 0;
my @f;

while (1) {
    @f = ();
    sleep 30;
    foreach $i (0 .. $n-1) {
	system ("./stat.pl $p $i");
    };
    foreach $i (0 .. $n-1) {
	push @f, "$r/out$i";
    };
    sleep 10;
    $s = system ("./stable.pl @f");
    if ((($s >> 8) & 0xFF) == 17) {
	exit;
    }
}
