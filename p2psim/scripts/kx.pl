#!/usr/bin/perl -w

# for ChurnEventGenerator
my $lifemean = 100000;
my $deathmean = $lifemean;
my $lookupmean = 10000; # 10000 for churn, 100 for lookup
my $exittime = 200000;
my $protocol = "Kelips";
my $nnodes = 1837; # Jinyang uses mostly 1024, also 1837
my $diameter = 100; # diameter of Euclidean universe
my $prefix = ""; # prefix to executable
my $observer = 1;
my $king = "";


&process_args(@ARGV);
@ARGV = ();

# if defined, use this rather than random euclidean.
if($king eq "" && $nnodes == 1024) {
    $king = "/home/am4/jinyang/chord/sfsnet/p2psim/oldking1024-t";
} elsif($king eq "" && $nnodes == 1837){
    $king = "/home/am4/jinyang/chord/sfsnet/p2psim/oldking1837-t";
} else {
    print STDERR "kx.pl: no king for $nnodes nodes\n";
}



# Run $protocol with many different parameter settings on the
# standard ChurnGenerator workload.
# Each line of output is x=bytes y=latency.

# name, min, max

my $kelips_k = int(sqrt($nnodes));
my $param_lists = {
  "Kelips" =>
    [
     [ "round_interval", 125, 500, 2000, 8000, 24000 ],
     [ "group_targets", 1, 4, 16, 64 ],
     [ "contact_targets", 1, 4, 16 ],
     [ "group_ration", 1, 4, 16, 64 ],
     [ "contact_ration", 1, 4, 16 ],
     [ "n_contacts", 2, 4, 8 ],
     [ "item_rounds", 0, 1, 4 ],
     [ "timeout", 5000, 10000, 20000, 40000 ],
     [ "k", $kelips_k ],
    ],

  "Kademlia" =>
    [
     [ "alpha", 1, 2, 3, 4, 5 ],
     [ "k", 8, 16, 32 ],
     [ "stabilize_timer", 2000, 4000, 8000, 16000, 32000 ],
     [ "refresh_rate",   2000, 4000, 8000, 16000, 32000 ],
    ],
};

my $params = $param_lists->{$protocol};

my $pf = "pf$$";
my $tf = defined($king) ? $king : "tf$$";
my $ef = "ef$$";

$| = 1;


print "# nnodes $nnodes dia $diameter life $lifemean death $deathmean lookup $lookupmean exit $exittime\n";
if(defined($king)){
    print "# king $king\n";
}

print "# cols: bytes/node/second lat hops ";
my $pi;
for($pi = 0; $pi <= $#$params; $pi++){
    my $name = $params->[$pi][0];
    print "$name ";
}
print "\n";

my $iters;
for($iters = 0; $iters < 500; $iters++){
    print "# ";
    open(PF, ">$pf");
    print PF "$protocol ";
    my $pi;
    my %pv;
    for($pi = 0; $pi <= $#$params; $pi++){
        my $name = $params->[$pi][0];
        my $np = $#{$params->[$pi]};
        my $x = $params->[$pi][1 + int(rand($np))];
        print PF "$name=$x ";
        print "$name=$x ";
        $pv{$name} = $x;
    }
    print PF "\n";
    print "\n";
    close(PF);

    if(!defined($king)){
        open(TF, ">$tf");
        print TF "topology Euclidean\n";
        print TF "failure_model NullFailureModel\n";
        print TF "\n";
        my $ni;
        for($ni = 1; $ni <= $nnodes; $ni++){
            printf(TF "$ni %d,%d\n", int(rand($diameter)), int(rand($diameter)));
        }
        close(TF);
    }

    open(EF, ">$ef");
    print EF "generator ChurnEventGenerator proto=$protocol ipkeys=1 lifemean=$lifemean deathmean=$deathmean lookupmean=$lookupmean exittime=$exittime\n";
    $observer and print EF "observer $protocol" . "Observer initnodes=1\n";
    close(EF);

    my $bytes;
    my $lat;
    my $hops;

    print "# $prefix./p2psim $pf $tf $ef\n";
    open(P, "$prefix./p2psim $pf $tf $ef |");
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

    printf("%f $lat $hops ", $bytes / ($nnodes * $exittime));
    for($pi = 0; $pi <= $#$params; $pi++){
        my $name = $params->[$pi][0];
        printf("%s ", $pv{$name});
    }
    print "\n";
}


#
# Processes arguments and sets options.
#
sub process_args {
  my @args = @_;

  while(@args) {
    $_ = shift @args;
    if(/^-d$/ || /^--deathmean$/){ $deathmean = shift @args; next; }
    if(/^-e$/ || /^--exittime$/) { $exittime = shift @args; next; }
    if(/^-h$/ || /^--help$/)     { &usage(); exit; }
    if(/^-k$/ || /^--king$/)     { $king = shift @args; next; }
    if(/^-l$/ || /^--lifemean$/) { $lifemean = shift @args; next; }
    if(/^-n$/ || /^--nnodes$/)   { $nnodes = shift @args; next; }
    if(/^-o$/ || /^--observer$/) { $observer = 0; next; }
    if(/^-p$/ || /^--protocol$/) { $protocol = shift @args; next; }
    if(/^-x$/ || /^--prefix$/)   { $prefix = shift @args; next; }
  }
}

sub usage {
  print <<EOF;
-d : deathmean
-e : exittime
-h : help
-k : king file
-l : lifemean
-n : nnodes
-o : NO observer
-p : protocol
-x : prefix
EOF
}
