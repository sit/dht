## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.16 2003/03/07 21:35:37 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = lsd

# noinst_HEADERS =

lsd_SOURCES = lsd.C

lsd_LDADD = ../dhash/libdhash.a ../merkle/libmerkle.a ../chord/libchord.a \
            ../utils/libutil.a ../svc/libsvc.la $(DBLIB) ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

