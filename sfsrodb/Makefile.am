## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.15 2005/08/19 20:04:33 fdabek Exp $

bin_PROGRAMS = sfsrodb 

noinst_HEADERS = \
sfsrodb.h sfsrodb_core.h

sfsrodb_SOURCES = \
sfsrodb.C sfsrodb_core.C 

sfsrodb_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
