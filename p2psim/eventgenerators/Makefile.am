noinst_PROGRAMS = eventgenerators.o
eventgenerators_o_SOURCES = churneventgenerator.C eventgeneratorfactory.C \
			fileeventgenerator.C sillyeventgenerator.C \
			churnfileeventgenerator.C

EXTRA_DIST = churneventgenerator.h churnfileeventgenerator.h eventgeneratorfactory.h \
             fileeventgenerator.h sillyeventgenerator.h

LIBS =
eventgenerators_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
