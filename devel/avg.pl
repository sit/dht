#! /usr/bin/perl

$nodes = shift;
$pernode = shift;

$sum = 0;
$count = 0;
while (<ARGV>) {
    $sum += $_;
    $count++;
}

print $nodes*$pernode," ",$sum/$count,"\n";
