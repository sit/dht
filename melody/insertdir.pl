#!/usr/local/bin/perl

sub get_mp3s {
	my ($dir, $recursive) = @_;

	my $path;
	my @files;

	unless (opendir(DIR, $dir)) {
		warn "Can't open $dir\n";
		closedir(DIR);
		return;
	}

	    foreach (readdir(DIR)) {
		next if $_ eq '.' || $_ eq '..';
		$path = "$dir/$_";
		next if (-l $path);
		if (-d $path) {		# a directory
		    if (defined ($recursive) && $recursive == 1) {
			push(@files, get_mp3s($path, $recursive));
		    }
		} elsif (-f _) {	# a plain file
		    if (/\.mp3$/i) {
			push(@files,$path); 
		    }
		}
	    }
	closedir(DIR);

	return @files;
}

if(@ARGV != 5) {
    die "usage: insertdir.pl localdir number_of_dirs_to_strip_off_front destdir remote_host remote_port\n";
}

$pathstrip = $ARGV[1];
@mp3s = get_mp3s($ARGV[0], 1);
foreach $mp3file (@mp3s) {
    $mp3file =~ /\/?(?:[^\/]+\/){$pathstrip}(.*\/)[^\/]+$/;
    print "$ARGV[2]/$1 $mp3file\n";
    `it '$mp3file' '$ARGV[2]/$1' $ARGV[3] $ARGV[4] 2> /dev/null`;
}
