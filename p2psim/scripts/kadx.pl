#!/usr/bin/perl -w

use FileHandle;

my $diameter = 100;
my $params = {
  NODES => {
    MIN => 32,
    MAX => 1024,
    INC => "*=2",
  },

  STAB_TIMER => {
    MIN => 2000,
    MAX => 32000,
    INC => "*=2",
  },

  K => {
    MIN => 4,
    MAX => 32,
    INC => "*=2",
  },

  ALPHA => {
    MIN => 1,
    MAX => 5,
    INC => "+=1",
  },
};


sub generate_topology
{
  my ($nnodes) = @_;

  # topology file
  my $tf = new FileHandle(">kademlia-top.txt") or die "$!";
  print $tf "topology Euclidean\n";
  print $tf "failure_model NullFailureModel\n";
  print $tf "\n";
  for(my $ni = 1; $ni <= $nnodes; $ni++) {
    printf($tf "$ni %d,%d\n", int(rand($diameter)), int(rand($diameter)));
  }
  $tf->close();
}


sub generate_events
{
  my ($nnodes) = @_;

  my $ef = new FileHandle(">kademlia-events.txt") or die "$!";
  print $ef "generator ChurnEventGenerator proto=Kademlia seed=0\n";
  $ef->close();
}

sub run_kademlia
{
  open(P, "p2psim/p2psim kademlia-prot.txt kademlia-top.txt kademlia-events.txt |");
  my $totlatency = 0;
  my $nlatencies = 0;
  my $bytes = 0;
  while(<P>){
      if(/^latency ([0-9]+)/){
          $totlatency += $1;
          $nlatencies++;
      }
      if(/^lookup ([0-9.]+)/){
          $bytes += $1;
      }
      if(/^using seed ([0-9.]+)/){
          print "Seed = $1\n";
      }
  }
  close(P);

  if(!$bytes || !$totlatency){
      print STDERR "kadx.pl: p2psim no output\n";
  }

  print sprintf("%.2f ", $totlatency/$nlatencies), $bytes, "\n";
}

sub generate_prot
{
  my ($nnodes) = @_;

  my $stabtimer = $params->{STAB_TIMER}->{MIN};
  while($stabtimer <= $params->{STAB_TIMER}->{MAX}) {
    my $k = $params->{K}->{MIN};
    while($k <= $params->{K}->{MAX}) {
      my $alpha = $params->{ALPHA}->{MIN};
      while($alpha <= $params->{ALPHA}->{MAX}) {
      print "nnodes=$nnodes, stabtimer=$stabtimer, k=$k, alpha=$alpha\n";
        my $pf = new FileHandle(">kademlia-prot.txt") or die "$!";
        print $pf "Kademlia k=$k alpha=$alpha stabilize_timer=$stabtimer\n";
        $pf->close();

        &run_kademlia();
        eval "\$alpha " . $params->{ALPHA}->{INC} . ";"
      }
      eval "\$k " . $params->{K}->{INC} . ";"
    }
    eval "\$stabtimer " . $params->{STAB_TIMER}->{INC} . ";"
  }
}


$nnodes = $params->{NODES}->{MIN};
while($nnodes <= $params->{NODES}->{MAX}) {
  &generate_topology($nnodes);
  &generate_events($nnodes);
  &generate_prot($nnodes);

  eval "\$nnodes " . $params->{NODES}->{INC} . ";"
}
