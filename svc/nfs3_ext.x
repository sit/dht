/* $Id: nfs3_ext.x,v 1.1 2001/01/18 16:21:50 fdabek Exp $ */

/*
 *
 * Copyright (C) 1999 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */


%struct fattr3exp {
%  ftype3 type;
%  u_int32_t mode;
%  u_int32_t nlink;
%  u_int32_t uid;
%  u_int32_t gid;
%  u_int64_t size;
%  u_int64_t used;
%  specdata3 rdev;
%  u_int64_t fsid;
%  u_int64_t fileid;
%  nfstime3 atime;
%  nfstime3 mtime;
%  nfstime3 ctime;
%  u_int32_t expire;
%
%  fattr3exp () : expire (0) {}
%  operator fattr3 &() { return *reinterpret_cast<fattr3 *> (this); }
%  operator const fattr3 &() const
%    { return *reinterpret_cast<const fattr3 *> (this); }
%  fattr3exp &operator= (const fattr3 &f)
%    { implicit_cast<fattr3 &> (*this) = f; return *this; }
%};
%template<class T> bool
%rpc_traverse (T &t, fattr3exp &obj)
%{
%  return rpc_traverse (t, implicit_cast<fattr3 &> (obj));
%}
%#ifdef MAINTAINER
%inline const strbuf &
%rpc_print (const strbuf &sb, const fattr3exp &obj, int recdepth,
%           const char *name, const char *prefix)
%{
%  if (name) {
%    if (prefix)
%      sb << prefix;
%    sb << "fattr3exp " << name << " = ";
%  }
%  const char *sep;
%  str npref;
%  if (prefix) {
%    npref = strbuf ("%s  ", prefix);
%    sep = "";
%    sb << "{\n";
%  }
%  else {
%    sep = ", ";
%    sb << "{ ";
%  }
%  rpc_print (sb, obj.type, recdepth, "type", npref);
%  sb << sep;
%  rpc_print (sb, obj.mode, recdepth, "mode", npref);
%  sb << sep;
%  rpc_print (sb, obj.nlink, recdepth, "nlink", npref);
%  sb << sep;
%  rpc_print (sb, obj.uid, recdepth, "uid", npref);
%  sb << sep;
%  rpc_print (sb, obj.gid, recdepth, "gid", npref);
%  sb << sep;
%  rpc_print (sb, obj.size, recdepth, "size", npref);
%  sb << sep;
%  rpc_print (sb, obj.used, recdepth, "used", npref);
%  sb << sep;
%  rpc_print (sb, obj.rdev, recdepth, "rdev", npref);
%  sb << sep;
%  rpc_print (sb, obj.fsid, recdepth, "fsid", npref);
%  sb << sep;
%  rpc_print (sb, obj.fileid, recdepth, "fileid", npref);
%  sb << sep;
%  rpc_print (sb, obj.atime, recdepth, "atime", npref);
%  sb << sep;
%  rpc_print (sb, obj.mtime, recdepth, "mtime", npref);
%  sb << sep;
%  rpc_print (sb, obj.ctime, recdepth, "ctime", npref);
%  sb << sep;
%  rpc_print (sb, obj.expire, recdepth, "expire", npref);
%  if (prefix)
%    sb << prefix << "};\n";
%  else
%    sb << " }";
%  return sb;
%}
%#endif /* MAINTAINER */

#define fattr3 fattr3exp
