## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.4 2004/10/01 21:32:06 sit Exp $

$(PROGRAMS): $(LDEPS) 

bin_PROGRAMS = usenet

usenet_SOURCES = usenet.C nntp.C group.C config.C newspeer.C
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
