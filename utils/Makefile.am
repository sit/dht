## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.1 2003/03/07 20:16:45 sit Exp $

noinst_LIBRARIES = libutil.a

noinst_HEADERS = ida.h
libutil_a_SOURCES = ida.C
# should really figure out how to encode explict rule so that make
# will run ida-genfield.py to update ida-field.C if ida-genfield.py changes.

TESTS = test_ida
check_PROGRAMS = $(TESTS)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a ../chord/modlogger.o $(LIBSFSCRYPT) $(LIBASYNC) $(LIBGMP)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
