## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.19 2004/03/24 02:16:24 benjie Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .x .C .h
.x.h:
	-$(RPCC) -h $< || rm -f $@
.x.C:
	-$(RPCC) -c $< || rm -f $@
$(RPCC):
	@:

bin_PROGRAMS = lsdctl lsd

lsdctl_prot.h: lsdctl_prot.x
lsdctl_prot.C: lsdctl_prot.x
lsdctl_prot.o: lsdctl_prot.h

$(DEP_FILES): lsdctl_prot.h
noinst_HEADERS = lsdctl_prot.x

lsd_SOURCES = lsdctl_prot.C lsd.C

lsd_LDADD = ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

lsdctl_SOURCES = lsdctl_prot.C lsdctl.C


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

