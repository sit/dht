## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2002/06/11 21:32:14 jsr Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = melody


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C
melody_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

