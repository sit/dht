## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libchord.a

noinst_HEADERS = aclnt_chord.h \
	         chord.h \
		 chord_impl.h \
		 chord_util.h \
		 comm.h \
		 debruijn.h \
		 finger_table.h \
		 fingerlike.h \
		 location.h \
		 pred_list.h \
		 route.h \
		 route_secchord.h \
		 stabilize.h \
		 succ_list.h \
		 toe_table.h

libchord_a_SOURCES = chord.C \
		      route.C \
		      route_secchord.C \
		      stabilize.C \
		      chord_client.C \
		      chord_util.C \
		      server.C \
		      comm.C \
		      location.C \
		      toe_table.C \
		      finger_table.C \
		      succ_list.C \
		      pred_list.C \
		      debruijn.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
