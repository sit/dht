noinst_PROGRAMS = protocols.o
protocols_o_SOURCES = chord.C chordfinger.C chordfingerpns.C \
		      chordonehop.C chordtoe.C kademlia.C \
		      koorde.C pastry.C protocolfactory.C tapestry.C \
                      kelips.C sillyprotocol.C

EXTRA_DIST = chord.h chordfinger.h chordfingerpns.h chordonehop.h \
             chordtoe.h consistenthash.h kademlia.h kelips.h koorde.h \
             protocolfactory.h sillyprotocol.h tapestry.h

LIBS =
protocols_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
