# 
# Async.pm
# 
# Copyright (C) 2002  Thomer M. Gil (thomer@lcs.mit.edu)
#   		       Massachusetts Institute of Technology
# 
#  Permission is hereby granted, free of charge, to any person obtaining
#  a copy of this software and associated documentation files (the
#  "Software"), to deal in the Software without restriction, including
#  without limitation the rights to use, copy, modify, merge, publish,
#  distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so, subject to
#  the following conditions:
# 
#  The above copyright notice and this permission notice shall be
#  included in all copies or substantial portions of the Software.
# 
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
#  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
#  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# 

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
