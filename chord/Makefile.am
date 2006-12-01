## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libchord.a

noinst_HEADERS = aclnt_chord.h \
	         chord.h \
		 chord_impl.h \
		 comm.h \
		 finger_table.h \
		 fingerroute.h \
		 pred_list.h \
		 route.h \
		 recroute.h \
		 recroute.C \
		 route_recchord.h \
		 stabilize.h \
		 succ_list.h \
		 finger_table_pns.h \
		 fingerroutepns.h \
		 accordion_table.h \
		 accordion.h 

libchord_a_SOURCES = chord.C \
		      chord_client.C \
		      comm.C \
		      finger_table.C \
		      fingerroute.C \
		      pred_list.C \
		      route.C \
		      route_chord.C \
		      route_recchord.C \
		      server.C \
		      stabilize.C \
		      succ_list.C \
		      finger_table_pns.C \
		      fingerroutepns.C  \
		      stp_manager.C \
		      accordion.C \
		      accordion_table.C \
		      route_accordion.C

CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
