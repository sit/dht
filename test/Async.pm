# package Async;

use Fcntl;
use IO::Socket;
use IO::Handle;
use IO::Select;

BEGIN {
  ($selread, $selwrite) = (0,1);
}
my $rfd = IO::Select->new();
my $wfd = IO::Select->new();
my %cb = ();


# sets callback
sub fdcb {
  my ($fd, $op, $sub, $params) = @_;

  if(!defined($sub) || $sub =~ m/^0$/ || $sub eq "") {
    $op == $selread and $rfd->remove($fd);
    $op == $selwrite and $wfd->remove($fd);
    delete $cb{$op};
    return;
  }

  $cb{$fd}{$op}{SUB} = $sub;
  $cb{$fd}{$op}{PARAMS} = $params;
  $op == $selread and $rfd->add($fd);
  $op == $selwrite and $wfd->add($fd);
}

sub amain() {
  # main event loop
  while(1) {
    my ($nr, $nw) = IO::Select->select($rfd, $wfd, undef);
    @$nr = () unless defined $nr;
    @$nw = () unless defined $nw;

    # handle writes
    for($i=0; $i<(scalar @$nw); $i++) {
      my $method = $cb{$$nw[$i]}{$selwrite}{SUB};
      my $params = $cb{$$nw[$i]}{$selwrite}{PARAMS};
      &$method($params);
    }

    # handle reads
    for($i=0; $i<(scalar @$nr); $i++) {
      my $method = $cb{$$nr[$i]}{$selread}{SUB};
      my $params = $cb{$$nr[$i]}{$selread}{PARAMS};
      &$method($params);
    }
  }
}

1;
