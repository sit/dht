noinst_PROGRAMS = events.o
events_o_SOURCES = eventfactory.C netevent.C \
                      p2pevent.C simevent.C

EXTRA_DIST = eventfactory.h netevent.h p2pevent.h simevent.h

LIBS =
events_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
