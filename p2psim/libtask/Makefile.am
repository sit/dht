noinst_PROGRAMS = libtask.o
libtask_o_SOURCES = \
	asm.S \
	channel.c \
	context.c \
	print.c \
	task.c 

EXTRA_DIST = task.h taskimpl.h 386-ucontext.h power-ucontext.h \
	tprimes.c COPYRIGHT README
LIBS =
libtask_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
