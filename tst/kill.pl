#!/usr/bin/perl

$p = $ARGV[0];

open (PS, , '/bin/ps -awx |') or die "cannot run ps\n";
while (defined ($line = <PS>)) {
    if ($line =~ /$p/) {
      if (!($line =~ /sh -c/)) {
        if (!($line =~ /grep/)) {
          ($pid) = ($line =~ /(\d+)/);
          kill 9, $pid;
        }
      }
    }
};
