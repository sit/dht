## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2001/01/16 22:00:10 fdabek Exp $

$(PROGRAMS): $(LDEPS)

bin_PROGRAMS = lsd

noinst_HEADERS = \
sfsp2p.h

lsd_SOURCES = lsd.C client.C

lsd_LDADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

