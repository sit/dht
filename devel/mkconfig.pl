#! /usr/bin/perl

$num = @ARGV[0];

$port = 9001;
$idmax = 10000;

for ($i = 0 ; $i < $num ; $i++) {
    open(OUT, ">cf$i");
    $id = int(rand($idmax));
    print OUT "myport $port\n";
    print OUT "myID $id\n";
    print OUT "wellknownport 9000\n";
    print OUT "wellknownhost sled.lcs.mit.edu\n";
}
