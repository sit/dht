## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2004/08/26 21:19:23 sit Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = usenet

usenet_SOURCES = usenet.C nntp.C group.C config.C newspeer.C
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
