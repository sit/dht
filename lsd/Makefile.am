## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.17 2003/10/14 22:47:11 athicha Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = lsd

# noinst_HEADERS =

lsd_SOURCES = lsd.C

lsd_LDADD = ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la $(DBLIB) ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

