/* $Id: genfs_prot.x,v 1.1 2001/01/18 16:21:50 fdabek Exp $ */

#include "nfs3_prot.h"

struct genfs_fh {
	opaque data<NFS3_FHSIZE>;
};

struct genfs_attr {
	ftype3 type;
	uint32 mode;
	uint32 nlink;
	uint32 uid;
	uint32 gid;
	uint64 size;
	uint64 used;
	specdata3 rdev;
	uint64 fsid;
	uint64 fileid;
	nfstime3 atime;
	nfstime3 mtime;
	nfstime3 ctime;
	uint32 expire;
};

#if RFC_SYNTAX
union genfs_opt_attr switch (bool present) {
case TRUE:
	genfs_attr attributes;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef genfs_attr *genfs_opt_attr;
#endif /* !RFC_SYNTAX */

typedef pre_op_attr genfs_opt_wattr;

struct genfs_wcc {
	genfs_opt_wattr before;
	genfs_opt_attr after;
};

#if RFC_SYNTAX
union genfs_opt_fh switch (bool present) {
case TRUE:
	genfs_fh handle;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef genfs_fh *genfs_opt_fh;
#endif /* !RFC_SYNTAX */

struct genfs_diropargs {
	genfs_fh dir;
	filename3 name;
};

struct genfs_diropresok {
	genfs_opt_fh obj;
	genfs_opt_attr obj_attributes;
	genfs_wcc dir_wcc;
};

union genfs_diropres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_diropresok resok;
default:
	genfs_wcc resfail;	/* Directory attributes  */
};

union genfs_wccstat switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	genfs_wcc wcc;
};

union genfs_getattrres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_attr attributes;
default:
	void;
};

struct genfs_setattrargs {
	genfs_fh object;
	sattr3 new_attributes;
	sattrguard3 guard;
};

struct genfs_lookupresok {
	genfs_fh object;
	genfs_opt_attr obj_attributes;
	genfs_opt_attr dir_attributes;
};

union genfs_lookupres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_lookupresok resok;
default:
	genfs_opt_attr resfail;	/* Directory attributes */
};

struct genfs_accessargs {
	genfs_fh object;
	uint32 access;
};

struct genfs_accessresok {
	genfs_opt_attr obj_attributes;
	uint32 access;
};

union genfs_accessres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_accessresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_readlinkresok {
	genfs_opt_attr symlink_attributes;
	nfspath3 data;
};

union genfs_readlinkres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_readlinkresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_readargs {
	genfs_fh file;
	uint64 offset;
	uint32 count;
};

struct genfs_readresok {
	genfs_opt_attr file_attributes;
	uint32 count;
	bool eof;
	opaque data<>;
};

union genfs_readres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_readresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_writeargs {
	genfs_fh file;
	uint64 offset;
	uint32 count;
	stable_how stable;
	opaque data<>;
};

struct genfs_writeresok {
	genfs_wcc file_wcc;
	uint32 count;
	stable_how committed;
	writeverf3 verf;
};

union genfs_writeres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_writeresok resok;
default:
	genfs_wcc resfail;
};

struct genfs_createargs {
	genfs_diropargs where;
	createhow3 how;
};

struct genfs_mkdirargs {
	genfs_diropargs where;
	sattr3 attributes;
};

struct genfs_symlinkargs {
	genfs_diropargs where;
	symlinkdata3 symlink;
};

struct devicedata3 {
	sattr3 dev_attributes;
	specdata3 spec;
};

struct genfs_mknodargs {
	genfs_diropargs where;
	mknoddata3 what;
};

struct genfs_renameargs {
	genfs_diropargs from;
	genfs_diropargs to;
};

struct genfs_renamewcc {
	genfs_wcc fromdir_wcc;
	genfs_wcc todir_wcc;
};

union genfs_renameres switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	genfs_renamewcc res;
};

struct genfs_linkargs {
	genfs_fh file;
	genfs_diropargs link;
};

struct genfs_linkwcc {
	genfs_opt_attr file_attributes;
	genfs_wcc linkdir_wcc;
};

union genfs_linkres switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	genfs_linkwcc res;
};

struct genfs_readdirargs {
	genfs_fh dir;
	uint64 cookie;
	cookieverf3 cookieverf;
	uint32 count;
};

struct genfs_readdirresok {
	genfs_opt_attr dir_attributes;
	cookieverf3 cookieverf;
	dirlist3 reply;
};

union genfs_readdirres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_readdirresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_readdirplusargs {
	genfs_fh dir;
	uint64 cookie;
	cookieverf3 cookieverf;
	uint32 dircount;
	uint32 maxcount;
};

struct genfs_entryplus {
	uint64 fileid;
	filename3 name;
	uint64 cookie;
	genfs_opt_attr name_attributes;
	genfs_opt_fh name_handle;
	genfs_entryplus *nextentry;
};

struct genfs_dirlistplus {
	genfs_entryplus *entries;
	bool eof;
};

struct genfs_readdirplusresok {
	genfs_opt_attr dir_attributes;
	cookieverf3 cookieverf;
	genfs_dirlistplus reply;
};

union genfs_readdirplusres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_readdirplusresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_fsstatresok {
	genfs_opt_attr obj_attributes;
	uint64 tbytes;
	uint64 fbytes;
	uint64 abytes;
	uint64 tfiles;
	uint64 ffiles;
	uint64 afiles;
	uint32 invarsec;
};

union genfs_fsstatres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_fsstatresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_fsinforesok {
	genfs_opt_attr obj_attributes;
	uint32 rtmax;
	uint32 rtpref;
	uint32 rtmult;
	uint32 wtmax;
	uint32 wtpref;
	uint32 wtmult;
	uint32 dtpref;
	uint64 maxfilesize;
	nfstime3 time_delta;
	uint32 properties;
};

union genfs_fsinfores switch (nfsstat3 status) {
case NFS3_OK:
	genfs_fsinforesok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_pathconfresok {
	genfs_opt_attr obj_attributes;
	uint32 linkmax;
	uint32 name_max;
	bool no_trunc;
	bool chown_restricted;
	bool case_insensitive;
	bool case_preserving;
};

union genfs_pathconfres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_pathconfresok resok;
default:
	genfs_opt_attr resfail;
};

struct genfs_commitargs {
	genfs_fh file;
	uint64 offset;
	uint32 count;
};

struct genfs_commitresok {
	genfs_wcc file_wcc;
	writeverf3 verf;
};

union genfs_commitres switch (nfsstat3 status) {
case NFS3_OK:
	genfs_commitresok resok;
default:
	genfs_wcc resfail;
};

program GENFS_PROGRAM {
	version GENFSNFS_VERS {
		void
		GENFS_NULL (void) = 0;
		
		genfs_getattrres
		GENFS_GETATTR (genfs_fh) = 1;
		
		genfs_wccstat
		GENFS_SETATTR (genfs_setattrargs) = 2;
		
		genfs_lookupres
		GENFS_LOOKUP (genfs_diropargs) = 3;
		
		genfs_accessres
		GENFS_ACCESS (genfs_accessargs) = 4;
		
		genfs_readlinkres
		GENFS_READLINK (genfs_fh) = 5;
		
		genfs_readres
		GENFS_READ (genfs_readargs) = 6;
		
		genfs_writeres
		GENFS_WRITE (genfs_writeargs) = 7;
		
		genfs_diropres
		GENFS_CREATE (genfs_createargs) = 8;
		
		genfs_diropres
		GENFS_MKDIR (genfs_mkdirargs) = 9;
		
		genfs_diropres
		GENFS_SYMLINK (genfs_symlinkargs) = 10;
		
		genfs_diropres
		GENFS_MKNOD (genfs_mknodargs) = 11;
		
		genfs_wccstat
		GENFS_REMOVE (genfs_diropargs) = 12;
		
		genfs_wccstat
		GENFS_RMDIR (genfs_diropargs) = 13;
		
		genfs_renameres
		GENFS_RENAME (genfs_renameargs) = 14;
		
		genfs_linkres
		GENFS_LINK (genfs_linkargs) = 15;
		
		genfs_readdirres
		GENFS_READDIR (genfs_readdirargs) = 16;
		
		genfs_readdirplusres
		GENFS_READDIRPLUS (genfs_readdirplusargs) = 17;
		
		genfs_fsstatres
		GENFS_FSSTAT (genfs_fh) = 18;
		
		genfs_fsinfores
		GENFS_FSINFO (genfs_fh) = 19;
		
		genfs_pathconfres
		GENFS_PATHCONF (genfs_fh) = 20;
		
		genfs_commitres
		GENFS_COMMIT (genfs_commitargs) = 21;
	} = 1;
} = 344428;
