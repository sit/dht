/* $Id: nfs3exp_prot.x,v 1.1 2001/01/18 16:21:51 fdabek Exp $ */

/*
 * This file contains the NFS3 protocol with a single change: The
 * ex_fattr3 contains an extra field called expire.  Any data structures
 * that do not depend on ex_fattr3 are omitted.
 */

#ifndef RFC_SYNTAX
# define RFC_SYNTAX 1
#endif /* RFC_SYNTAX */

#ifdef RPCC
# ifndef UNION_ONLY_DEFAULT
#  define UNION_ONLY_DEFAULT 1
# endif /* UNION_ONLY_DEFAULT */
#endif

%#include "nfs3_prot.h"

struct ex_fattr3 {
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
	uint32 expire;		/* This is different from NFS3 */
};

#if RFC_SYNTAX
union ex_post_op_attr switch (bool present) {
case TRUE:
	ex_fattr3 attributes;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef ex_fattr3 *ex_post_op_attr;
#endif /* !RFC_SYNTAX */

struct ex_wcc_data {
	pre_op_attr before;
	ex_post_op_attr after;
};

struct ex_diropres3ok {
	post_op_fh3 obj;
	ex_post_op_attr obj_attributes;
	ex_wcc_data dir_wcc;
};

union ex_diropres3 switch (nfsstat3 status) {
case NFS3_OK:
	ex_diropres3ok resok;
default:
	ex_wcc_data resfail;	/* Directory attributes  */
};

union ex_wccstat3 switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	ex_wcc_data wcc;
};

union ex_getattr3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_fattr3 attributes;
default:
	void;
};

struct ex_lookup3resok {
	nfs_fh3 object;
	ex_post_op_attr obj_attributes;
	ex_post_op_attr dir_attributes;
};

union ex_lookup3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_lookup3resok resok;
default:
	ex_post_op_attr resfail;	/* Directory attributes */
};

struct ex_access3resok {
	ex_post_op_attr obj_attributes;
	uint32 access;
};

union ex_access3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_access3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_readlink3resok {
	ex_post_op_attr symlink_attributes;
	nfspath3 data;
};

union ex_readlink3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_readlink3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_read3resok {
	ex_post_op_attr file_attributes;
	uint32 count;
	bool eof;
	opaque data<>;
};

union ex_read3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_read3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_write3resok {
	ex_wcc_data file_wcc;
	uint32 count;
	stable_how committed;
	writeverf3 verf;
};

union ex_write3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_write3resok resok;
default:
	ex_wcc_data resfail;
};

struct ex_rename3wcc {
	ex_wcc_data fromdir_wcc;
	ex_wcc_data todir_wcc;
};

union ex_rename3res switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	ex_rename3wcc res;
};

struct ex_link3wcc {
	ex_post_op_attr file_attributes;
	ex_wcc_data linkdir_wcc;
};

union ex_link3res switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	ex_link3wcc res;
};

struct ex_readdir3resok {
	ex_post_op_attr dir_attributes;
	cookieverf3 cookieverf;
	dirlist3 reply;
};

union ex_readdir3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_readdir3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_entryplus3 {
	uint64 fileid;
	filename3 name;
	uint64 cookie;
	ex_post_op_attr name_attributes;
	post_op_fh3 name_handle;
	ex_entryplus3 *nextentry;
};

struct ex_dirlistplus3 {
	ex_entryplus3 *entries;
	bool eof;
};

struct ex_readdirplus3resok {
	ex_post_op_attr dir_attributes;
	cookieverf3 cookieverf;
	ex_dirlistplus3 reply;
};

union ex_readdirplus3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_readdirplus3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_fsstat3resok {
	ex_post_op_attr obj_attributes;
	uint64 tbytes;
	uint64 fbytes;
	uint64 abytes;
	uint64 tfiles;
	uint64 ffiles;
	uint64 afiles;
	uint32 invarsec;
};

union ex_fsstat3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_fsstat3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_fsinfo3resok {
	ex_post_op_attr obj_attributes;
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

union ex_fsinfo3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_fsinfo3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_pathconf3resok {
	ex_post_op_attr obj_attributes;
	uint32 linkmax;
	uint32 name_max;
	bool no_trunc;
	bool chown_restricted;
	bool case_insensitive;
	bool case_preserving;
};

union ex_pathconf3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_pathconf3resok resok;
default:
	ex_post_op_attr resfail;
};

struct ex_commit3resok {
	ex_wcc_data file_wcc;
	writeverf3 verf;
};

union ex_commit3res switch (nfsstat3 status) {
case NFS3_OK:
	ex_commit3resok resok;
default:
	ex_wcc_data resfail;
};

program ex_NFS_PROGRAM {
	version ex_NFS_V3 {
		void
		ex_NFSPROC3_NULL (void) = 0;
		
		ex_getattr3res
		ex_NFSPROC3_GETATTR (nfs_fh3) = 1;
		
		ex_wccstat3
		ex_NFSPROC3_SETATTR (setattr3args) = 2;
		
		ex_lookup3res
		ex_NFSPROC3_LOOKUP (diropargs3) = 3;
		
		ex_access3res
		ex_NFSPROC3_ACCESS (access3args) = 4;
		
		ex_readlink3res
		ex_NFSPROC3_READLINK (nfs_fh3) = 5;
		
		ex_read3res
		ex_NFSPROC3_READ (read3args) = 6;
		
		ex_write3res
		ex_NFSPROC3_WRITE (write3args) = 7;
		
		ex_diropres3
		ex_NFSPROC3_CREATE (create3args) = 8;
		
		ex_diropres3
		ex_NFSPROC3_MKDIR (mkdir3args) = 9;
		
		ex_diropres3
		ex_NFSPROC3_SYMLINK (symlink3args) = 10;
		
		ex_diropres3
		ex_NFSPROC3_MKNOD (mknod3args) = 11;
		
		ex_wccstat3
		ex_NFSPROC3_REMOVE (diropargs3) = 12;
		
		ex_wccstat3
		ex_NFSPROC3_RMDIR (diropargs3) = 13;
		
		ex_rename3res
		ex_NFSPROC3_RENAME (rename3args) = 14;
		
		ex_link3res
		ex_NFSPROC3_LINK (link3args) = 15;
		
		ex_readdir3res
		ex_NFSPROC3_READDIR (readdir3args) = 16;
		
		ex_readdirplus3res
		ex_NFSPROC3_READDIRPLUS (readdirplus3args) = 17;
		
		ex_fsstat3res
		ex_NFSPROC3_FSSTAT (nfs_fh3) = 18;
		
		ex_fsinfo3res
		ex_NFSPROC3_FSINFO (nfs_fh3) = 19;
		
		ex_pathconf3res
		ex_NFSPROC3_PATHCONF (nfs_fh3) = 20;
		
		ex_commit3res
		ex_NFSPROC3_COMMIT (commit3args) = 21;
	} = 3;
} = 344444;

struct ex_invalidate3args {
	nfs_fh3 handle;
	ex_post_op_attr attributes;
};

program ex_NFSCB_PROGRAM {
	version ex_NFSCB_V3 {
		void
		ex_NFSCBPROC3_NULL (void) = 0;

		void
		ex_NFSCBPROC3_INVALIDATE (ex_invalidate3args) = 1;
	} = 3;
}= 344445;
