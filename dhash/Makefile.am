## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.16 2005/03/07 01:30:29 sit Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a libdhashclient.a 

libdhashclient_a_SOURCES = dhashclient.C verify.C

noinst_HEADERS = dhash.h dhash_common.h dhash_impl.h dhashcli.h dhashclient.h dhashgateway.h download.h verify.h vsc.h dhash_store.h succopt.h pmaint.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C download.C dhash_store.C dhash_fetchrec.C pmaint.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

