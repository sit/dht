## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libchord.a

noinst_HEADERS = chord.h chord_util.h location.h stabilize.h

libchord_a_SOURCES = chord.C stabilize.C chord_client.C chord_util.C server.C \
          comm.C location.C toe_table.C finger_table.C succ_list.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
