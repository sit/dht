## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.15 2003/01/06 20:06:34 cates Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = lsd

# noinst_HEADERS =

lsd_SOURCES = lsd.C

lsd_LDADD = ../chord/libchord.a  ../dhash/libdhash.a ../merkle/libmerkle.a \
            ../svc/libsvc.la $(DBLIB) ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

