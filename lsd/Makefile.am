## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.18 2004/03/05 12:06:04 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .x .C .h
.x.h:
	-$(RPCC) -h $< || rm -f $@
.x.C:
	-$(RPCC) -c $< || rm -f $@
$(RPCC):
	@:

bin_PROGRAMS = lsd lsdctl

lsdctl_prot.h: lsdctl_prot.x
lsdctl_prot.C: lsdctl_prot.x
lsdctl_prot.o: lsdctl_prot.h

$(DEP_FILES): lsdctl_prot.h
noinst_HEADERS = lsdctl_prot.x

lsd_SOURCES = lsd.C lsdctl_prot.C

lsd_LDADD = ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

lsdctl_SOURCES = lsdctl.C lsdctl_prot.C


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

