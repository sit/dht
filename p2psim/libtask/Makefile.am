noinst_PROGRAMS = libtask.o
libtask_o_SOURCES = 386.c\
                 386label.s\
                 channel.c\
                 emalloc.c\
                 idle.c\
                 print.c\
                 q.c\
                 sched.c

EXTRA_DIST = task.h taskimpl.h
LIBS =
libtask_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
