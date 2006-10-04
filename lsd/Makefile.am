## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.30 2006/10/04 23:05:09 strib Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .T .x .C .h
.T.C:
	-$(TAME) -o $@ $< || rm -f $@

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
sample_server.C: sample_server.T
sampler.C: sampler.T
sampled_LDADD =  ../dhash/libdhash.a \
		 ../chord/libchord.a ../utils/libutil.a ../devel/rpclib.o \
		 ../svc/libsvc.la  \
	         $(DBLIB) ${LDADD} 

lsdctl_SOURCES = lsdctl.C
lsdctl_LDADD = ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore $(bin_SCRIPTS)
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

