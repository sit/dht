noinst_PROGRAMS = libtask.o
libtask_o_SOURCES = \
	asm.S \
	channel.c \
	context.c \
	print.c \
	task.c 

EXTRA_DIST = task.h taskimpl.h tprimes.c COPYRIGHT
LIBS =
libtask_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
