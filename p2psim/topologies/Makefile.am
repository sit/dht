noinst_PROGRAMS = topologies.o
topologies_o_SOURCES = gtitm.C dvgraph.C e2egraph.C euclidean.C \
                          euclideangraph.C g2graph.C randomgraph.C \
			  topologyfactory.C e2easymgraph.C \
			  e2elinkfailgraph.C e2etimegraph.C

LIBS =
topologies_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
