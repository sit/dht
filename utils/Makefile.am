## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.4 2003/03/15 23:07:43 sit Exp $

noinst_LIBRARIES = libutil.a

noinst_HEADERS = \
	coord.h \
	ida.h \
	modlogger.h \
	skiplist.h

libutil_a_SOURCES = \
	coord.C \
	ida.C \
	modlogger.C

# should really figure out how to encode explict rule so that make
# will run ida-genfield.py to update ida-field.C if ida-genfield.py changes.

TESTS = test_ida test_skiplist
check_PROGRAMS = $(TESTS)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_skiplist_SOURCES = test_skiplist.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
