## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2002/01/10 17:11:31 fdabek Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

noinst_HEADERS = dhash.h

libdhash_a_SOURCES = server.C client.C verify.C

#libdhash_a_LIBADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

