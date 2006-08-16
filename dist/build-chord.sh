#!/bin/sh

BUILDROOT=$1
PUBLISHROOT=$2

if [ -z "$BUILDROOT" ]; then
    echo "Must specify a build root!" 1>&2
    exit 1
fi

# Give up if anything goes bad.
set -ex

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

cvs -d /home/am0/sfsnetcvs co sfsnet
cd sfsnet
patch <<'EOP'
--- setup       26 Feb 2003 01:09:14 -0000      1.2
+++ setup       16 Aug 2006 15:07:24 -0000
@@ -85,7 +85,7 @@
 set -x
 chmod +x setup
 libtoolize $LTI_ARGS
-aclocal
+aclocal -I /usr/local/share/aclocal
 autoheader
 automake $AM_ARGS
 autoconf
EOP
PKG_CONFIG_PATH=:/usr/local/libdata/pkgconfig:/usr/X11R6/libdata/pkgconfig PATH=/usr/local/gnu-autotools/bin:$PATH ./setup
./configure
gmake distcheck CXX="ccache g++" CC="ccache gcc"
gmake dist-bzip2
if [ -d "$PUBLISHROOT" ]; then
    cp chord-0.1.tar.bz2 $PUBLISHROOT/chord-0.1-$today.tar.bz2
    chmod 644 $PUBLISHROOT/chord-0.1-$today.tar.bz2
fi
rm -rf sfsnet
