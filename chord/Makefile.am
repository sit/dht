## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libchord.a

noinst_HEADERS = aclnt_chord.h \
	         chord.h \
		 chord_impl.h \
		 comm.h \
		 debruijn.h \
		 finger_table.h \
		 fingerroute.h \
		 pred_list.h \
		 proxroute.h \
		 route.h \
		 route_secchord.h \
		 stabilize.h \
		 succ_list.h \
		 toe_table.h \
		 finger_table_pns.h

libchord_a_SOURCES = chord.C \
		      chord_client.C \
		      comm.C \
		      debruijn.C \
		      finger_table.C \
		      fingerroute.C \
		      pred_list.C \
		      proxroute.C \
		      route.C \
		      route_chord.C \
		      route_debruijn.C \
		      server.C \
		      stabilize.C \
		      succ_list.C \
		      toe_table.C \
		      finger_table_pns.C \
		      fingerroutepns.C 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
