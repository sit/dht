package ChordTestHarness;

use strict;
use POSIX qw(:signal_h :sys_wait_h);

BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

    $VERSION = 1.00;

    @ISA = qw(Exporter);
    @EXPORT = qw(guessip);
}

sub guessip {
    die "Can't guess a good IP to use.\n" unless ($^O eq "freebsd");
    my $DEFINT = (grep { /interface/ } split(/\n/, `/sbin/route get default`))[0]
	|| "lo0";
    $DEFINT =~ s/ *interface: //;
    my $MYIP = (grep {/inet / } split (/\n/, `/sbin/ifconfig $DEFINT`))[0];
    $MYIP = (split (/\s+/, $MYIP))[2];
    return $MYIP;
}

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $self = {};
    $self->{build}  = shift;
    $self->{outdir} = shift;
    $self->{confs}  = {};
    $self->{pids}   = {};

    bless ($self, $class);
    return $self;
}

sub spawnlsd {
    my $self = shift;
    my $conf = shift;
    warn "Re-using configuration $conf; this is bad.\n"
	if defined $self->{confs}->{$conf};

    my $rundir = "$self->{outdir}/lsd-$conf";

    mkdir $rundir, 0755 unless -d $rundir;

    my @args = ("$self->{build}/lsd/lsd",
		"-d", "./db",
		"-S", "./sock",
		"-C", "./ctlsock",
		);
    push @args, @_ if @_;
    # xxx save arguments?

    if (my $pid = fork ()) {
	# parent
        print "RUNNING conf $conf ($pid): lsd @args\n";
	$self->{confs}->{$conf} = $pid; # for caller
	$self->{pids}->{$pid} = $conf;  # for us
	return $pid;
    } else {
	# $ENV{SHORT_STATS} = 1; # avoid extraneous RPC statistics; see comm.C
	# $ENV{CHORD_RPC_STYLE} = 0; # default STP for RPCs; see location.C
	chdir $rundir or die "Couldn't chdir $rundir: $!\n";
	open (PID, "> pid"); print PID $$; close (PID);
	open (STDOUT, "> log")
	    || die "Couldn't re-open STDOUT: $!\n";
	open (STDERR, ">&STDOUT")
	    || die "Couldn't dup STDOUT: $!\n";
	print "RUNNING: @args\n";
	exec @args
	    or die "Couldn't exec: $!\n";
    }
}

sub killlsd {
    ## kill a specified lsd 
    ##  XXX this code is cheezy

    my $self = shift;
    my $conf = shift;
    my $pid = $self->getpid ($conf);

    kill SIGINT, $pid;
    my $reapedpid = waitpid (-1, WNOHANG);
    die "WTF: REAPED $reapedpid, EXPECTED $pid" if ($pid != $reapedpid);
    sleep 5;
    kill SIGKILL, $pid;

    delete $self->{pids}->{$pid};
    delete $self->{confs}->{$conf};
}



sub reaplsds {
    my $self = shift;
    my $n = scalar keys %{$self->{pids}};
    return unless $n; # anything to reap?
    my $hit = kill SIGINT, $self->pids (); # encourage graceful exit
    warn "Apparently, only $hit of $n lsd's remaining...\n" unless $hit == $n;

    my $pid = 0;
    while ($pid != -1) {
	$pid = waitpid (-1, WNOHANG);
	next unless $pid > 0;
	my $signalno = $? & 127;
	my $coredumped = $? & 128;

	print "Conf $self->{pids}->{$pid} ($pid) returned ", $? >> 8, 
	  ($signalno   ? ", signal $signalno" : ""),
	  ($coredumped ? ", dumped core" : ""), ".\n";

	my $c =	delete $self->{pids}->{$pid};
	die "Assert: rep inv violated.\n" unless defined $c;
	my $p = delete $self->{confs}->{$c};
	die "Assert: rep inv violated.\n" unless $p == $pid;
    }
    # Deal with stubborn children forcefully.
    for (keys %{$self->{pids}}) {
	warn "Did not collect child $_; disciplining.\n";
	kill SIGKILL, $_;
    }
    # xxx should timeout on waitpid; say, after 10 total seconds of waiting.
    # xxx should reap children after we discipline them.
}

sub pids {
    my $self = shift;
    return sort keys %{$self->{pids}};
}

sub getpid {
    my $self = shift;
    my $conf = shift;
    return $self->{confs}->{$conf};
}

sub store {
    my $self = shift;
    my $conf = shift;
    my $vnode = shift;
    my $count = shift || 1000;
    my $size = 4; # bytes
    my $seed = 0;
    my $log = shift || "store.log";
    $self->dbm ($vnode, "lsd-$conf/sock", $count, $size, $seed, "s", $log);
}

sub fetch {
    my $self = shift;
    my $conf = shift;
    my $vnode = shift;
    my $count = shift || 1000;
    my $size = 4; # bytes
    my $seed = 0;
    my $log = shift || "fetch.log";
    $self->dbm ($vnode, "lsd-$conf/sock", $count, $size, $seed, "f", $log);
}

# rewrites relative paths to be relative to outdir.
sub dbm {
    my $self = shift;

    my $vnode = shift;
    my $lsdsock = shift;
    $lsdsock = "$self->{outdir}/$lsdsock" unless $lsdsock =~ m,^/,;
    my $count = shift;
    my $size = shift;
    my $seed = shift;
    my $fetch = shift;
    my $log = shift || "dbm.log";
    $log = "$self->{outdir}/$log" unless $log =~ m,^/,;
    my $s = system ("$self->{build}/devel/dbm", $vnode, $lsdsock,
		    $count, $size, $log, $fetch, "0", $seed);
    return $s;
}

1;
