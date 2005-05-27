## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.24 2005/05/27 13:30:27 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = lsdctl lsd syncd

lsd_SOURCES = lsd.C

lsd_LDADD = ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

syncd_SOURCES = syncd.C syncer.C syncer.h

syncd_LDADD =  ../merkle/libmerkle.a \
	     ../utils/libutil.a ../devel/rpclib.o \
		../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

lsdctl_SOURCES = lsdctl.C
lsdctl_LDADD = ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

