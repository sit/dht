## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2003/01/18 13:12:57 jsr Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

noinst_HEADERS = dhash.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C route_dhash.C dhashclient.C

#libdhash_a_LIBADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

