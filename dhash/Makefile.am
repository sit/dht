## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.10 2003/06/07 16:47:50 fdabek Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

noinst_HEADERS = dhash.h dhash_common.h dhash_impl.h dhashcli.h dhashclient.h dhashgateway.h download.h verify.h vsc.h dhash_store.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C dhashclient.C download.C dhash_store.C

#libdhash_a_LIBADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

