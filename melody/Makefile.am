## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.12 2003/02/19 22:42:56 sit Exp $

$(PROGRAMS): $(LDEPS) 

if GTK_AVAILABLE
DONATE = donation_plugin
endif

noinst_PROGRAMS = melody it googleit $(DONATE)


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C retrieve_manager.C
melody_LDADD = ../dhash/libdhash.a ../merkle/libmerkle.a ../svc/libsvc.la ${LDADD}

it_SOURCES = it.C cs_output.C
googleit_SOURCES = googleit.C
if GTK_AVAILABLE
donation_plugin_SOURCES = donation_plugin.C
INCLUDES = $(GTK_CFLAGS)
donation_plugin_LDFLAGS = -pthread
donation_plugin_LDADD = -lxmms $(GTK_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

