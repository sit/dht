#!/usr/bin/perl -w

# Run Kelips with many different parameter settings on the
# standard ChurnGenerator workload.
# Each line of output is x=bytes y=latency.

# name, min, max
my $params =
    [
     [ "round_interval", 125, 500, 2000, 8000, 24000 ],
     [ "group_targets", 1, 4, 16, 64 ],
     [ "contact_targets", 1, 4, 16 ],
     [ "group_ration", 1, 4, 16, 64 ],
     [ "contact_ration", 1, 4, 16 ],
     [ "n_contacts", 2, 4, 8 ],
     [ "item_rounds", 0, 1, 4 ],
     [ "timeout", 5000, 10000, 20000, 40000 ]
     ];

my $nnodes = 256;
my $k = int(sqrt($nnodes));
my $diameter = 100; # diameter of Euclidean universe

# for ChurnEventGenerator
my $lifemean = 100000;
my $deathmean = $lifemean;
my $lookupmean = 10000;
my $exittime = 200000;

my $pf = "pf$$";
my $tf = "tf$$";
my $ef = "ef$$";

$| = 1;

print "# n $nnodes k $k dia $diameter life $lifemean ";
print "death $deathmean lookup $lookupmean exit $exittime\n";

my $iters;
for($iters = 0; $iters < 200; $iters++){
    print "# ";
    open(PF, ">$pf");
    print PF "Kelips k=$k ";
    my $pi;
    for($pi = 0; $pi <= $#$params; $pi++){
        my $name = $params->[$pi][0];
        my $np = $#{$params->[$pi]};
        my $x = $params->[$pi][1 + int(rand($np))];
        print PF "$name=$x ";
        print "$name=$x ";
    }
    print PF "\n";
    print "\n";
    close(PF);

    open(TF, ">$tf");
    print TF "topology Euclidean\n";
    print TF "failure_model NullFailureModel\n";
    print TF "\n";
    my $ni;
    for($ni = 1; $ni <= $nnodes; $ni++){
        printf(TF "$ni %d,%d\n", int(rand($diameter)), int(rand($diameter)));
    }
    close(TF);

    open(EF, ">$ef");
    print EF "generator ChurnEventGenerator proto=Kelips ipkeys=1 lifemean=$lifemean deathmean=$deathmean lookupmean=$lookupmean exittime=$exittime\n";
    print EF "observer KelipsObserver initnodes=1\n";
    close(EF);

    my $bytes;
    my $lat;
    my $hops;

    open(P, "./p2psim $pf $tf $ef |");
    while(<P>){
        if(/^rpc_bytes ([0-9]+)/){
            $bytes = $1;
        }
        if(/^avglat ([0-9.]+)/){
            $lat = $1;
        }
        if(/avghops ([0-9.]+)/){
            $hops = $1;
        }
        if(/^([0-9]+) good, ([0-9]+) ok failures, ([0-9]+) bad f/){
            print "# $1 good, $2 ok failures, $3 bad failures\n";
        }
    }
    close(P);

    if(!defined($bytes) || !defined($lat)){
        print STDERR "kx.pl: p2psim no output\n";
    }

    print "$bytes $lat $hops\n";
}
