#!/usr/bin/perl -w 

use FileHandle;
use Data::Dumper;


my $verbose = 0;
my %values = ();
my @hosts = ();
my @tests = ();
my $CF = "config.txt";
my $killall = 1;
my $dryrun = 0;
my $suppress = 1;
my $quiet = 1;
my $confsave = 0;

my $SSH = "ssh";
my $SLEEP = "5";


sub usage {
  print <<EOF;
Usage: testsetup [-c ] [-d] [-h] [-k] [-q] [-s] [-v] CONFIG_FILE

-c,--confsave / +c  :  don't delete generated conf file, default off
-d,--dryrun   / +d  :  don't really do anything
-h,--help           :  show this help
-k,--killall  / +k  :  killall testslaves first, default on
-q,--quiet    / +q  :  suppress stdout/stderr of tests, default on
-s,--suppress / +s  :  suppress stdout/stderr of running processes, default on
-v,--verbose  / +v  :  blah blah blah blah blah blah blah
EOF
}

sub uniqueport {
  my ($hosts, $host, $key) = @_;

  NEW_PORT: while(1) {
    my $port = int(rand(16384)) + 2048;
    foreach (@$hosts) {
      if($host->{NAME} eq $_->{NAME} and $port == $_->{$key}) {
        next NEW_PORT;
      }
    }
    return $port;
  }
}

sub main {
  &process_args(@ARGV);
  @ARGV = ();
  srand(time ^ ($$ + ($$ << 15)));

  #
  #  Read the config file
  #
  my $fh = new FileHandle("<$CF") or die "open: $!";
  my $line = 0;
  while(<$fh>) {
    $line++;
    chomp;

    #
    # comment
    #
    if(/^\s*#/ || /^\s*$/) {
      next;
    #
    # Makefile-style variable assignment
    #
    } elsif(/^\s*(\w+)\s*=\s*(\S+)\s*$/) {
      $1 eq "SSH" and $SSH = $2 and next;
      $1 eq "SLEEP" and $SLEEP = $2 and next;
      $values{$1} = $2;
      next;
    #
    # Test case
    #
    } elsif (/^\s*(\S+)$/) {
      push @tests, $_;
      next;
    }

    #
    # Substitute keys for their values, if defined in %values
    #
    my @keys = split /\s+/;
    my $n = 0;
    for(my $i = 0; $keys[$i]; $i++) {
      $keys[$i] and $n++;
      next unless $keys[$i] =~ m/\$(\w+)/;
      exists $values{$1} or warn "undefined variable $1\n";
      $keys[$i] = $values{$1};
    }

    if($n != 4) {
      warn "line $line of $CF is invalid. skipping...";
      next;
    }

    my ($hostname, $teslapath, $lsdpath, $slavepath) = @keys;

    my $host = { NAME => $hostname,
                 LSD => $lsdpath,
                 TESLA => $teslapath,
                 SLAVE => $slavepath,
               };
    $verbose >= 2 and print "$CF: $hostname\n";

    #
    # pick a random, unique port for the tesla control port and lsd port
    #
    $host->{TESLAPORT} = &uniqueport(\@hosts, $host, "TESLAPORT");
    $host->{LSDPORT} = &uniqueport(\@hosts, $host, "LSDPORT");
    $host->{SLAVEPORT} = &uniqueport(\@hosts, $host, "SLAVEPORT");
    push @hosts, $host;
  }
  $fh->close();
  $verbose >= 2 and print Dumper(@hosts);

  #
  # well-known lsd
  #
  my $master = "$hosts[0]->{NAME}:$hosts[0]->{LSDPORT}";
  $verbose >= 2 and print "master is $master\n";

  #
  # killall testslaves
  #
  if($killall) {
    foreach my $h (@hosts) {
      my $cmd =  "kill -9 \`ps -ax | egrep \"testslave|lsd\" | awk '{print \$1}'\`";
      &do_execute($h->{NAME}, $cmd);
    }
  }

  #
  # Start the lsd nodes
  #
  foreach my $h (@hosts) {
    my $cmd = "$h->{TESLA} +dhashtest -port=$h->{TESLAPORT} $h->{LSD} -p $h->{LSDPORT} -j $master -l $h->{NAME} -S /tmp/lsd_socket-$$-$h->{LSDPORT}";
    &do_execute($h->{NAME}, $cmd);
  }

  #
  # Start the slaves
  #
  foreach my $h (@hosts) {
    $h->{SOCKETFILE} = "/tmp/lsd_socket-$$-$h->{LSDPORT}";
    my $cmd = "$h->{SLAVE} -p $h->{SLAVEPORT} -s $h->{SOCKETFILE}";
    &do_execute($h->{NAME}, $cmd);
  }

  #
  # Generate an input file for the test case
  #
  my $cf = "/tmp/dhashtest-$$-config.txt";
  my $cfh = new FileHandle(">$cf") or die "open: $!";
  foreach my $h (@hosts) {
    print $cfh "$h->{NAME} $h->{SLAVEPORT} $h->{TESLAPORT}\n";
  }
  $cfh->close();

  #
  # Let stuff settle down
  #
  sleep $SLEEP;

  #
  # Run the tests
  #
  foreach (@tests) {
    $verbose >= 1 and print "running test: $_ $cf\n";
    my $cmd = "$_ $cf";
    $quiet and $cmd .= " >/dev/null 2>&1";
    unless($dryrun) {
      print "$_ : ";
      if(system "$cmd") {
        print "FAILED";
      } else {
        print "PASSED";
      }
    } else {
      print "$_ : [DRYRUN]";
    }
    print "\n";
  }

  #
  # Clean up the mess
  #
  $confsave or unlink $cf;
  foreach my $h (@hosts) {
    unlink $h->{SOCKETFILE};
  }
}

sub do_execute {
  my ($host, $cmd) = @_;
  unless($host eq "localhost" || $host =~ m/^127/) {
    $cmd = "$SSH $host->{NAME} $cmd";
  }
  $suppress and $cmd .= " > /dev/null 2>&1";
  $verbose >= 2 and print "executing: $cmd\n";
  $dryrun or system "$cmd &";
}

sub process_args {
  my @args = @_;

  while(@args) {
    $_ = shift @args;
    if(/^-c/ || /^--conf-save/)   { $confsave++; next; }
    if(/^\+c/)                    { $confsave--; next; }
    if(/^-d/ || /^--dryrun/)      { $dryrun++; next; }
    if(/^\+d/)                    { $dryrun--; next; }
    if(/^-h/ || /^--help/)        { &usage(); exit; }
    if(/^-k/ || /^--killall/)     { $killall++; next; }
    if(/^\+k/)                    { $killall--; next; }
    if(/^-q/ || /^--quiet/)       { $quiet++; next; }
    if(/^\+q/)                    { $quiet--; next; }
    if(/^-s/ || /^--suppress/)    { $suppress++; next; }
    if(/^\+s/)                    { $suppress--; next; }
    if(/^-v/ || /^--verbose/)     { $verbose++; next; }
    if(/^\+v/)                    { $verbose--; next; }
    $CF = $_;
  }

  $verbose >= 2 and print "config file = $CF\n";
}

&main();
