## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.40 2005/08/17 03:51:45 fdabek Exp $


BASEDIRS = svc utils merkle chord dhash sfsrodb devel lsd
SUBDIRS_ALL = $(BASEDIRS) chordcd test tst melody proxy usenet arpeggio amud
SUBDIRS_SOME = $(BASEDIRS)
if COND_ALL
SUBDIRS = $(SUBDIRS_ALL)
else
SUBDIRS = $(SUBDIRS_SOME)
endif

chordinclude_HEADERS = config.h

CLEANFILES = core *.core *~

EXTRA_DIST = .cvsignore
MAINTAINERCLEANFILES = aclocal.m4 install-sh mkinstalldirs \
	missing depcomp configure config.guess config.sub config.h.in \
	ltconfig ltmain.sh INSTALL COPYING Makefile.in stamp-h.in

dist-hook:
	rm -f $(srcdir)/p2psim/p2psim.core
	tar czvf $(distdir)/p2psim.tgz -C $(srcdir) p2psim
	-cp -f depcomp $(distdir)


install-data-local:
	-src=$(DESTDIR)$(chorddatadir); dst=$(DESTDIR)$(pkgdatadir); \
        rm -f $$dst; \
        if test "`dirname $$src`" = "`dirname $$dst`"; then \
                ln -s "`basename $$src`" $$dst; \
        else \
                ln -s $(chorddatadir) $$dst; \
        fi
	-src=$(DESTDIR)$(chordincludedir); dst=$(DESTDIR)$(pkgincludedir); \
        rm -f $$dst; \
        if test "`dirname $$src`" = "`dirname $$dst`"; then \
                ln -s "`basename $$src`" $$dst; \
        else \
                ln -s $(chordincludedir) $$dst; \
        fi

install-exec-local:
	-src=$(DESTDIR)$(chordlibdir); dst=$(DESTDIR)$(pkglibdir); \
        rm -f $$dst; \
        if test "`dirname $$src`" = "`dirname $$dst`"; then \
                ln -s "`basename $$src`" $$dst; \
        else \
                ln -s $(chordlibdir) $$dst; \
        fi

uninstall-local:
	-rm -f $(DESTDIR)$(pkgdatadir) $(DESTDIR)$(chorddatadir)
	-rm -f $(DESTDIR)$(pkglibdir) $(DESTDIR)$(chordlibdir) 
	-rm -f $(DESTDIR)$(pkgincludedir) $(DESTDIR)$(chordincludedir) 
	-rmdir $(DESTDIR)$(confdir)
