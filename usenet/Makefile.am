## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2003/08/13 15:32:11 jsr Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = usenet

usenet_SOURCES = usenet.C
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
