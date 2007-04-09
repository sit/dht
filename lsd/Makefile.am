## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .T .C .h
.T.C:
	-$(TAME) -o $@ $< || rm -f $@

bin_PROGRAMS = lsdctl adbd lsd
bin_SCRIPTS = start-dhash

lsd_SOURCES = lsd.C

lsd_LDADD = ../dhash/libdhash.a ../merkle/libmerkle.a \
	    ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	    $(DBLIB) ${LDADD} 

adbd_SOURCES = adbd.C 
adbd_LDADD =  ../utils/libutil.a ../svc/libsvc.la ../merkle/libmerkle.a $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

lsdctl_SOURCES = lsdctl.C
lsdctl_LDADD = ../svc/libsvc.la $(LDADD)

TAMEOUT = sample_server.C sampler.C

EXTRA_DIST = $(bin_SCRIPTS)
CLEANFILES = core *.core *~ *.rpo $(TAMEOUT)
MAINTAINERCLEANFILES = Makefile.in

