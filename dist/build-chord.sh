#!/bin/sh

BUILDROOT=$1
PUBLISHROOT=$2

if [ -z "$BUILDROOT" ]; then
    echo "Must specify a build root!" 1>&2
    exit 1
fi

# Give up if anything goes bad.
set -e

umask 022

# save stdout and stderr
# exec 5>&1
# exec 6>&2

if [ ! -d "$BUILDROOT" ]; then
    mkdir $BUILDROOT
fi
today=$(date +%Y%m%d)

cd $BUILDROOT

# Send our own stdout and stderr to a log file
exec >build-$today.log
exec 2>&1

hg clone /home/am9/public-hg/dht.hg dht
cd dht
PKG_CONFIG_PATH=:/usr/local/libdata/pkgconfig:/usr/X11R6/libdata/pkgconfig PATH=/usr/local/gnu-autotools/bin:$PATH export PKG_CONFIG_PATH PATH
./setup
./configure
gmake distcheck DISTCHECK_CONFIGURE_FLAGS="--with-mode=shdbg" CXX="ccache g++" CC="ccache gcc"
gmake dist-bzip2
if [ -d "$PUBLISHROOT" ]; then
    cp chord-0.1.tar.bz2 $PUBLISHROOT/chord-0.1-$today.tar.bz2
    chmod 644 $PUBLISHROOT/chord-0.1-$today.tar.bz2
fi
cd .. && rm -rf dht
