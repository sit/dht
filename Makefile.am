## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.41 2006/08/16 17:03:54 sit Exp $


SUBDIRS = svc utils merkle chord dhash maint lsd tools
chordinclude_HEADERS = config.h

CLEANFILES = core *.core *~

MAINTAINERCLEANFILES = aclocal.m4 install-sh mkinstalldirs \
	missing depcomp configure config.guess config.sub config.h.in \
	ltconfig ltmain.sh INSTALL COPYING Makefile.in stamp-h.in

dist-hook:
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
