## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2003/08/18 19:05:49 jsr Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = usenet

usenet_SOURCES = usenet.C nntp.C group.C
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
