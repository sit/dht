noinst_PROGRAMS = protocols.o
protocols_o_SOURCES = chord.C chordfinger.C chordfingerpns.C \
		      chordonehop.C chordtoe.C kademlia.C \
		      koorde.C protocolfactory.C tapestry.C onehop.C\
                      kelips.C sillyprotocol.C \
		      accordion.C ratecontrolqueue.C 

EXTRA_DIST = chord.h chordfinger.h chordfingerpns.h chordonehop.h \
             chordtoe.h consistenthash.h kademlia.h kelips.h koorde.h \
             protocolfactory.h sillyprotocol.h tapestry.h onehop.h \
	     accordion.h ratecontrolqueue.h 

LIBS =
protocols_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
