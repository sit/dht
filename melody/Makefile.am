## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2003/01/07 05:17:34 jsr Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = melody it


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C retrieve_manager.C

it_SOURCES = it.C cs_output.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

