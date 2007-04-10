# Spec file for Chord

Summary: Chord/DHash -- a distributed hash table
Name: chord
Version: 0.1
Release: 4
Copyright: BSD 
Group: Applications/Internet
Source: http://pdos.csail.mit.edu/~fdabek/chord-0.1.tar.gz
URL: http://pdos.csail.mit.edu/chord/
Packager: Chord developers (chord@pdos.csail.mit.edu)
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
Requires: sfslite >= 0.8, db4 >= 4.0
BuildRequires: sfslite >= 0.8, db4 >= 4.0

%description
Chord and DHash are building blocks for developing distributed applications.
Chord and DHash together provide a distributed hash table implementation.

%package vis
Summary: Chord visualization utilities
Group: Applications/Internet

%description vis
Chord and DHash are building blocks for developing distributed applications.
This package provides the X11/Gtk visualizer.

%package devel
Summary: Chord/DHash development libraries and headers
Group: Applications/Internet

%description devel
Chord and DHash are building blocks for developing distributed applications.
This package provides the necessary development libraries and headers


%prep
%setup -q
%configure

%build
make

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%pre
%post


%files
%defattr(-,root,root)
%doc  README
%{_bindir}/dbdump
%{_bindir}/dbm
%{_bindir}/dbm.py
%{_bindir}/filestore
%{_bindir}/findroute
%{_bindir}/lsd
%{_bindir}/lsdctl
%{_bindir}/lsdping
%{_bindir}/maintd
%{_bindir}/maintwalk
%{_bindir}/merkledump
%{_bindir}/nodeq
%{_bindir}/nodeq-filter
%{_bindir}/start-dhash
%{_bindir}/walk
%{_bindir}/adbd
%{_includedir}/chord-%{version}/chord_types.x
%{_includedir}/chord-%{version}/dhash_types.x
%{_includedir}/chord-%{version}/dhashgateway_prot.x
%{_datadir}/chord-%{version}/bigint.py
%{_datadir}/chord-%{version}/chord_types.py
%{_datadir}/chord-%{version}/dhashgateway_prot.py
%{_datadir}/chord-%{version}/dhash_types.py
%{_datadir}/chord-%{version}/RPCProto.py
%{_datadir}/chord-%{version}/RPC.py

%files vis
%{_bindir}/vis
%{_datadir}/chord-%{version}/vischat.py

%files devel
%{_libdir}/chord
%{_includedir}/chord
%{_libdir}/chord-%{version}/libdhashclient.a
%{_libdir}/chord-%{version}/libsvc.a
%{_libdir}/chord-%{version}/libsvc.la
%{_libdir}/chord-%{version}/libutil.a
%{_libdir}/chord-%{version}/libadb.a
%{_includedir}/chord-%{version}/chord_types.h
%{_includedir}/chord-%{version}/config.h
%{_includedir}/chord-%{version}/configurator.h
%{_includedir}/chord-%{version}/dhashclient.h
%{_includedir}/chord-%{version}/dhash_common.h
%{_includedir}/chord-%{version}/dhashgateway_prot.h
%{_includedir}/chord-%{version}/dhash.h
%{_includedir}/chord-%{version}/dhash_prot.h
%{_includedir}/chord-%{version}/dhash_types.h
%{_includedir}/chord-%{version}/dhblock_chash.h
%{_includedir}/chord-%{version}/dhblock.h
%{_includedir}/chord-%{version}/dhblock_keyhash.h
%{_includedir}/chord-%{version}/dhblock_replicated.h
%{_includedir}/chord-%{version}/dhblock_noauth.h
%{_includedir}/chord-%{version}/id_utils.h
%{_includedir}/chord-%{version}/lsdctl_prot.h
%{_includedir}/chord-%{version}/modlogger.h
%{_includedir}/chord-%{version}/skiplist.h

%changelog
* Tue Apr 10 2007 Emil Sit <sit@mit.edu>
- maintwalk

* Thu Mar 15 2007 Emil Sit <sit@mit.edu>
- merkledump

* Sun Mar  4 2007 Emil Sit <sit@mit.edu>
- Nuke dbm_noauth.

* Tue Aug 30 2005 Emil Sit <sit@mit.edu>
- adbmigrate 

* Wed Aug  3 2005 Emil Sit <sit@mit.edu>
- dbm_noauth

* Wed Jul 20 2005 Emil Sit <sit@mit.edu>
- dhashping

* Tue May 24 2005 Emil Sit <sit@mit.edu>
- New dhblock header files

* Sat May 14 2005 Jeremy Stribling <strib@mit.edu>
- configurator.h and skiplist.h

* Sat May 14 2005 Jeremy Stribling <strib@mit.edu>
- devel package, for working with dhashclient

* Thu Apr 21 2005 Emil Sit <sit@mit.edu>
- vis package

* Thu Mar 10 2005 Emil Sit <sit@mit.edu>
- syncd!

* Sat Feb 26 2005 Emil Sit <sit@mit.edu>
- Add Frank's lsdping
- Bump package release number because of new merkle.

* Sun Feb 20 2005 Emil Sit <sit@mit.edu>
- Add udbctl for usenet control

* Wed Jan 26 2005 Emil Sit <sit@mit.edu>
- Include nodeq-filter

* Sat Jan 22 2005 Emil Sit <sit@mit.edu>
- Add some python stuff

* Sun Jan 02 2005 Emil Sit <sit@mit.edu>
- Don't link against special db4

* Fri Oct 01 2004 Emil Sit <sit@mit.edu>
- Add usenet

* Fri Jan 07 2004 Frank Dabek <fdabek@mit.edu>
- Initial SPEC
