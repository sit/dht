## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2005/01/29 17:33:40 athicha Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_LIBRARIES = libmud.a
noinst_PROGRAMS = amud sp minit
noinst_HEADERS = mud.h avatar.h room.h mud_obj.h thing.h

libmud_a_SOURCES = avatar.C game_engine.C room.C mud_misc.C 

amud_SOURCES = mud.C 
amud_LDADD = ./libmud.a ../dhash/libdhashclient.a  \
	../utils/libutil.a ../svc/libsvc.la $(LDADD)

sp_SOURCES = synplayer.C
sp_LDADD = ./libmud.a ../dhash/libdhashclient.a  \
	../utils/libutil.a ../svc/libsvc.la $(LDADD)

minit_SOURCES = mud_init.C
minit_LDADD = ./libmud.a ../dhash/libdhashclient.a  \
	../utils/libutil.a ../svc/libsvc.la $(LDADD)

#EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
