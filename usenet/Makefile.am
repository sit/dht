## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.5 2004/10/02 03:02:52 sit Exp $

$(PROGRAMS): $(LDEPS) 

bin_PROGRAMS = usenet

usenet_SOURCES = usenet.C nntp.C group.C config.C newspeer.C \
	usenet.h group.h newspeer.h nntp.h
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
