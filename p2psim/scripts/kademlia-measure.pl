#!/usr/bin/perl -w

use strict;

my @sizes = (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4196, 8192);

foreach my $s (@sizes) {
  my $less = $s-1;

  open FILE, ">kademlia.sc" or die $!;
  print FILE <<EOF;
protocol: Kademlia, k=20 alpha=3
     net: $s, Euclidean, random 1000 1000
 observe: 5000, reschedule=5000, numnodes=$s, initnodes=1
  events: 1000, 450000, 2000, constant, lookup, keylen=16

# not necessary because of initnodes
#  event: 1, join, wellknown=1
# events: $less, 100, 500, linear, join, wellknown=1

simulator 1000000 exit
EOF
  close FILE;

  system "perl sc.pl kademlia-prot.txt kademlia-top.txt kademlia-events.txt < kademlia.sc >/dev/null 2>&1";
  system "p2psim kademlia-prot.txt kademlia-top.txt kademlia-events.txt > /tmp/kademlia-measure-$$.txt";
  open FILE, "</tmp/kademlia-measure-$$.txt" or die $!;
  my $totallatency = 0;
  my $lines = 0;
  my $overhead = 0;
  while(<FILE>) {
    chomp;
    /latency = (\d+)/ and $totallatency += $1 and $lines++ and next;
    $overhead = $_;
  }
  close FILE;
  unlink "/tmp/kademlia-measure-$$.txt";

  my $avglat = $totallatency / $lines;
  print "$s\t$avglat\t$overhead\n";
}
