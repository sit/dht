# Spec file for Chord

Summary: Chord -- a distributed hash table
Name: chord
Version: 0.1
Release: 1
Copyright: BSD 
Group: Applications/Internet
Source: http://www.pdos.lcs.mit.edu/~fdabek/chord-0.1.tar.gz
URL: http://www.pdos.lcs.mit.edu/chord/
Packager: Chord developers (chord@pdos.lcs.mit.edu)
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
Requires: sfs >= 0.7.1
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
./configure --prefix=/usr --with-db=/home/yipal/db42-install

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
/usr/bin/lsd
/usr/bin/sfsrodb
/usr/bin/dbm
/usr/bin/filestore
/usr/bin/findroute
/usr/bin/lsdctl
/usr/bin/nodeq
/usr/bin/walk
/usr/bin/dbdump
/usr/bin/usenet

%changelog
* Fri Oct 01 2004 Emil Sit <sit@mit.edu>
- Add usenet

* Fri Jan 07 2004 Frank Dabek <fdabek@mit.edu>
- Initial SPEC
