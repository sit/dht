## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.10 2001/06/21 19:03:33 fdabek Exp $

$(PROGRAMS): $(LDEPS)

bin_PROGRAMS = lsd

noinst_HEADERS = \
chord.h chord_util.h

lsd_SOURCES = chord.C chord_client.C chord_util.C server.C lsd.C comm.C chord_callbacks.C

lsd_LDADD = ../dhash/libdhash.a ../svc/libsvc.la  $(DBLIB) ${LDADD} 


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

