$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhc.a

noinst_HEADERS = dhc.h

libdhc_a_SOURCES = dhc.C
libdhc_a_LIBADD = ../dhash/libdhash.a ${LDADD}
#../svc/libsvc.la 

EXTRA_DIST = 
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
