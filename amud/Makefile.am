## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2004/10/12 15:16:27 athicha Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = amud
noinst_HEADERS = mud.h avatar.h room.h mud_obj.h thing.h

amud_SOURCES = mud.C avatar.C game_engine.C room.C
amud_LDADD = ../dhash/libdhashclient.a  \
	../utils/libutil.a ../svc/libsvc.la $(LDADD)

#EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
