## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.13 2003/08/21 20:46:45 athicha Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a libdhashclient.a 

libdhashclient_a_SOURCES = dhashclient.C verify.C

noinst_HEADERS = dhash.h dhash_common.h dhash_impl.h dhashcli.h dhashclient.h dhashgateway.h download.h verify.h vsc.h dhash_store.h succopt.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C download.C dhash_store.C

#libdhash_a_LIBADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

