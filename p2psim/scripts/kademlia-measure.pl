#!/usr/bin/perl -w

use strict;

my @sizes = (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4196, 8192);

foreach my $s (@sizes) {
  my $less = $s-1;

  open FILE, ">kademlia.sc" or die $!;
  print FILE <<EOF;
protocol: Kademlia, k=20 alpha=3
     net: $s, Euclidean, random 0 0
# events: $less, 100, 500, linear, join, wellknown=1
 observe: 5000, reschedule=5000, numnodes=$s, initnodes=1
  events: 1000, 450000, 2000, constant, lookup, keylen=16
#  event: 1, join, wellknown=1

simulator 1000000 exit
EOF
  close FILE;

  system "perl sc.pl kademlia-prot.txt kademlia-top.txt kademlia-events.txt < kademlia.sc >/dev/null 2>&1";
  my $msg = `p2psim kademlia-prot.txt kademlia-top.txt kademlia-events.txt`;
  chomp $msg;
  print "$s\t$msg\n";
}
