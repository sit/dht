# Spec file for Chord

Summary: Chord/DHash -- a distributed hash table
Name: chord
Version: 0.1
Release: 3
Copyright: BSD 
Group: Applications/Internet
Source: http://pdos.csail.mit.edu/~fdabek/chord-0.1.tar.gz
URL: http://pdos.csail.mit.edu/chord/
Packager: Chord developers (chord@pdos.csail.mit.edu)
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
Requires: sfs >= 0.8, db4 >= 4.0
BuildRequires: sfs >= 0.8, db4 >= 4.0

%description
Chord and DHash are building blocks for developing distributed applications.
Chord and DHash together provide a distributed hash table implementation.
This package also includes the UsenetDHT server.

%package vis
Summary: Chord visualization utilities
Group: Applications/Internet

%description vis
Chord and DHash are building blocks for developing distributed applications.
This package provides the X11/Gtk visualizer.


%prep
%setup -q
%configure

%build
make SUBDIRS="\${BASEDIRS} usenet"

%install
rm -rf $RPM_BUILD_ROOT
make install SUBDIRS="\${BASEDIRS} usenet" DESTDIR=$RPM_BUILD_ROOT

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
%{_bindir}/nodeq
%{_bindir}/nodeq-filter
%{_bindir}/sfsrodb
%{_bindir}/syncd
%{_bindir}/usenet
%{_bindir}/udbctl
%{_bindir}/walk
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
%{_bindir}/usenetlsdmon.py
%{_datadir}/chord-%{version}/vischat.py


%changelog
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
