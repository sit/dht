## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.29 2006/10/03 19:11:43 strib Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = lsdctl lsd syncd sampled
bin_SCRIPTS = start-dhash

lsd_SOURCES = lsd.C

lsd_LDADD = ../dhash/libdhash.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

syncd_SOURCES = syncd.C syncer.C syncer.h
syncd_LDADD =  ../dhash/libdhash.a  ../merkle/libmerkle.a \
	     ../chord/libchord.a ../utils/libutil.a ../devel/rpclib.o \
		../svc/libsvc.la  \
	    $(DBLIB) ${LDADD} 

sampled_SOURCES = sampled.C sampler.C sampler.h sample_server.C sample_server.h
sampled_LDADD =  ../dhash/libdhash.a \
		 ../chord/libchord.a ../utils/libutil.a ../devel/rpclib.o \
		 ../svc/libsvc.la  \
	         $(DBLIB) ${LDADD} 

lsdctl_SOURCES = lsdctl.C
lsdctl_LDADD = ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore $(bin_SCRIPTS)
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

