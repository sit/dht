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
The Self-Certifying File System (SFS) is a secure, global file system
with completely decentralized control. SFS lets you access your files
from anywhere and share them with anyone, anywhere. Anyone can set up
an SFS server, and any user can access any server from any client. SFS
lets you share files across administrative realms without involving
administrators or certification authorities.

This file includes the core files necessary for SFS clients.  Also
included are libraries and header files useful for development of
SFS-enabled tools.


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
%{_bindir}/lsd
%{_bindir}/sfsrodb
%{_bindir}/dbm
%{_bindir}/filestore
%{_bindir}/findroute
%{_bindir}/lsdctl
%{_bindir}/nodeq
%{_bindir}/walk
%{_bindir}/dbdump
%{_bindir}/usenet

%changelog
* Sun Jan 02 2005 Emil Sit <sit@mit.edu>
- Don't link against special db4

* Fri Oct 01 2004 Emil Sit <sit@mit.edu>
- Add usenet

* Fri Jan 07 2004 Frank Dabek <fdabek@mit.edu>
- Initial SPEC
