## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.4 2001/02/19 02:52:40 kaashoek Exp $

$(PROGRAMS): $(LDEPS)

bin_PROGRAMS = lsd

noinst_HEADERS = \
chord.h chord_util.h chord_client.h

lsd_SOURCES = chord.C chord_client.C chord_util.C server.C lsd.C comm.C

lsd_LDADD = ../svc/libsvc.la ${LDADD} 

CXXFLAGS += -DSTATS

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

