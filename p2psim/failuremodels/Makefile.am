noinst_PROGRAMS = failuremodels.o
failuremodels_o_SOURCES = failuremodelfactory.C roundtripsfailuremodel.C \
		          constantfailuremodel.C

EXTRA_DIST = failuremodelfactory.h nullfailuremodel.h roundtripsfailuremodel.h \
	     constantfailuremodel.h

LIBS =
failuremodels_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
