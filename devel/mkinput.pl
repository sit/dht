#! /usr/bin/perl

$num = @ARGV[0];

for ($i = 0 ; $i < $num ; $i++) {
    $key = int(rand(10000));
    print "$key\n"
}
