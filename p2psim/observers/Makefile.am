noinst_PROGRAMS = observers.o
observers_o_SOURCES = kademliaobserver.C protocolobserver.C \
                    tapestryobserver.C chordobserver.C onehopobserver.C \
		    kelipsobserver.C observerfactory.C

EXTRA_DIST = chordobserver.h kademliaobserver.h kelipsobserver.h onehopobserver.h \
             observerfactory.h protocolobserver.h tapestryobserver.h

LIBS =
observers_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
