## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2003/01/06 22:56:11 jsr Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = melody it


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C

it_SOURCES = it.C cs_output.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

