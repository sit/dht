noinst_PROGRAMS = topologies.o
BASESOURCES = dvgraph.C e2egraph.C euclidean.C \
                          euclideangraph.C g2graph.C randomgraph.C \
			  topologyfactory.C e2easymgraph.C \
			  e2elinkfailgraph.C e2etimegraph.C

SOURCES_ALL = gtitm.C $(BASESOURCES)
SOURCES_SOME = $(BASESOURCES)

if BUILD_GTITM
topologies_o_SOURCES = $(SOURCES_ALL)
else
topologies_o_SOURCES = $(SOURCES_SOME)
endif

LIBS =
topologies_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
