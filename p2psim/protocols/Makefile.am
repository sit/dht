noinst_PROGRAMS = protocols.o
protocols_o_SOURCES = chord.C chordfinger.C chordfingerpns.C \
		      chordonehop.C chordtoe.C kademlia.C \
		      koorde.C pastry.C protocolfactory.C tapestry.C \
                      kelips.C
LIBS =
protocols_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
