## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2001/01/30 22:59:30 fdabek Exp $

$(PROGRAMS): $(LDEPS)

bin_PROGRAMS = lsd

noinst_HEADERS = \
sfsp2p.h

lsd_SOURCES = lsd.C client.C

lsd_LDADD = ../svc/libsvc.la ${LDADD} 

CXXFLAGS += -DSTATS

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

