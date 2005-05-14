## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.17 2005/05/14 06:35:04 strib Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

chordlib_LIBRARIES = libdhashclient.a
chordinclude_HEADERS = dhashclient.h dhash.h dhash_common.h verify.h

libdhashclient_a_SOURCES = dhashclient.C verify.C

noinst_HEADERS = dhash_impl.h dhashcli.h dhashgateway.h download.h vsc.h dhash_store.h succopt.h pmaint.h

libdhash_a_SOURCES = server.C client.C verify.C gateway.C download.C dhash_store.C dhash_fetchrec.C pmaint.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

