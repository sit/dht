## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) 

bin_PROGRAMS = ddnsq 

noinst_HEADERS = ddns.h

with_chord = ${top_builddir}

ddnsq_SOURCES = ddns.C ddnsq.C 
ddnsq_LDADD = ${with_chord}/chord/libchord.a \
	${with_chord}/dhash/libdhash.a \
	${with_chord}/svc/libsvc.la \
	${LDADD}

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
