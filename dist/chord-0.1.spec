# Spec file for Chord

Summary: Chord -- a distributed hash table
Name: chord
Version: 0.1
Release: 2
Copyright: BSD 
Group: Applications/Internet
Source: http://www.pdos.lcs.mit.edu/~fdabek/chord-0.1.tar.gz
URL: http://www.pdos.lcs.mit.edu/chord/
Packager: Chord developers (chord@pdos.lcs.mit.edu)
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
Requires: sfs >= 0.7.1, db4 >= 4.0
BuildRequires: sfs >= 0.7.1

%description
Chord and DHash are building blocks for developing distributed applications.
Chord and DHash together provide a distributed hash table implementation.
This package also includes the UsenetDHT server.

%prep
%setup -q
%configure

%build
make SUBDIRS="\${BASEDIRS} usenet"

%install
rm -rf $RPM_BUILD_ROOT
make install-strip SUBDIRS="\${BASEDIRS} usenet" DESTDIR=$RPM_BUILD_ROOT

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
%{_bindir}/nodeq
%{_bindir}/sfsrodb
%{_bindir}/usenet
%{_bindir}/usenetlsdmon.py
%{_bindir}/udbctl
%{_bindir}/walk
%{_bindir}/vis
%{_includedir}/chord-%{version}/chord_types.x
%{_includedir}/chord-%{version}/dhash_types.x
%{_includedir}/chord-%{version}/dhashgateway_prot.x
%{_datadir}/chord-%{version}/bigint.py
%{_datadir}/chord-%{version}/chord_types.py
%{_datadir}/chord-%{version}/dhashgateway_prot.py
%{_datadir}/chord-%{version}/dhash_types.py
%{_datadir}/chord-%{version}/RPCProto.py
%{_datadir}/chord-%{version}/RPC.py
%{_datadir}/chord-%{version}/vischat.py


%changelog
* Sun Feb 20 2005 Emil Sit <sit@mit.edu>
- Add udbctl for usenet control

* Sat Jan 22 2005 Emil Sit <sit@mit.edu>
- Add some python stuff

* Sun Jan 02 2005 Emil Sit <sit@mit.edu>
- Don't link against special db4

* Fri Oct 01 2004 Emil Sit <sit@mit.edu>
- Add usenet

* Fri Jan 07 2004 Frank Dabek <fdabek@mit.edu>
- Initial SPEC
