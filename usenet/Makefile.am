## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2005/02/18 21:34:33 sit Exp $

$(PROGRAMS): $(LDEPS) 

SUFFIXES = .x .C .h
.x.h:
	-$(RPCC) -h $< || rm -f $@
.x.C:
	-$(RPCC) -c $< || rm -f $@
$(RPCC):
	@:

usenetdht_storage.h: usenetdht_storage.x
usenetdht_storage.C: usenetdht_storage.x
usenetdht_storage.o: usenetdht_storage.h

$(DEP_FILES): usenetdht_storage.h
noinst_HEADERS = usenetdht_storage.x

bin_PROGRAMS = usenet udbctl

usenet_SOURCES = usenetdht_storage.C \
		 usenet.C nntp.C group.C config.C newspeer.C \
		 usenet.h group.h newspeer.h nntp.h
usenet_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ${DBLIB} ${LDADD}

udbctl_SOURCES = udbctl.C usenetdht_storage.C group.C
udbctl_LDADD = ${DBLIB} ${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
