## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2003/05/06 17:39:13 fdabek Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

noinst_HEADERS = dhash.h dhash_common.h dhash_impl.h dhashcli.h dhashclient.h dhashgateway.h route_dhash.h verify.h vsc.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C route_dhash.C dhashclient.C

#libdhash_a_LIBADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

