noinst_PROGRAMS = observers.o
observers_o_SOURCES = kademliaobserver.C protocolobserver.C \
                    tapestryobserver.C chordobserver.C  \
	            oldobserverfactory.C observerfactory.C \
		    kelipsobserver.C
LIBS =
observers_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
