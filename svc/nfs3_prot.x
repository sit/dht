/* $Id: nfs3_prot.x,v 1.1 2001/01/18 16:21:51 fdabek Exp $ */

#ifndef RFC_SYNTAX
# define RFC_SYNTAX 1
#endif /* RFC_SYNTAX */

#if 1
typedef unsigned hyper uint64;
typedef hyper int64;
typedef unsigned int uint32;
typedef int int32;
#else
#define uint64 unsigned hyper
#define int64 hyper
#define uint32 unsigned
#define int32 int
#endif

const NFS3_FHSIZE = 64;
const NFS3_COOKIEVERFSIZE = 8;
const NFS3_CREATEVERFSIZE = 8;
const NFS3_WRITEVERFSIZE = 8;

typedef string filename3<>;
typedef string nfspath3<>;
typedef opaque cookieverf3[NFS3_COOKIEVERFSIZE];
typedef opaque createverf3[NFS3_CREATEVERFSIZE];
typedef opaque writeverf3[NFS3_WRITEVERFSIZE];

enum nfsstat3 {
	NFS3_OK = 0,
	NFS3ERR_PERM = 1,
	NFS3ERR_NOENT = 2,
	NFS3ERR_IO = 5,
	NFS3ERR_NXIO = 6,
	NFS3ERR_ACCES = 13,
	NFS3ERR_EXIST = 17,
	NFS3ERR_XDEV = 18,
	NFS3ERR_NODEV = 19,
	NFS3ERR_NOTDIR = 20,
	NFS3ERR_ISDIR = 21,
	NFS3ERR_INVAL = 22,
	NFS3ERR_FBIG = 27,
	NFS3ERR_NOSPC = 28,
	NFS3ERR_ROFS = 30,
	NFS3ERR_MLINK = 31,
	NFS3ERR_NAMETOOLONG = 63,
	NFS3ERR_NOTEMPTY = 66,
	NFS3ERR_DQUOT = 69,
	NFS3ERR_STALE = 70,
	NFS3ERR_REMOTE = 71,
	NFS3ERR_BADHANDLE = 10001,
	NFS3ERR_NOT_SYNC = 10002,
	NFS3ERR_BAD_COOKIE = 10003,
	NFS3ERR_NOTSUPP = 10004,
	NFS3ERR_TOOSMALL = 10005,
	NFS3ERR_SERVERFAULT = 10006,
	NFS3ERR_BADTYPE = 10007,
	NFS3ERR_JUKEBOX = 10008
};

enum ftype3 {
	NF3REG = 1,
	NF3DIR = 2,
	NF3BLK = 3,
	NF3CHR = 4,
	NF3LNK = 5,
	NF3SOCK = 6,
	NF3FIFO = 7
};

struct specdata3 {
	uint32 major;
	uint32 minor;
};

struct nfs_fh3 {
	opaque data<NFS3_FHSIZE>;
};

struct nfstime3 {
	uint32 seconds;
	uint32 nseconds;
};

struct fattr3 {
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
};

#ifdef SFSSVC
# include "nfs3_ext.x"
#endif /* SFSSVC */

#if RFC_SYNTAX
union post_op_attr switch (bool present) {
case TRUE:
	fattr3 attributes;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef fattr3 *post_op_attr;
#endif /* !RFC_SYNTAX */

struct wcc_attr {
	uint64 size;
	nfstime3 mtime;
	nfstime3 ctime;
};

#if RFC_SYNTAX
union pre_op_attr switch (bool present) {
case TRUE:
	wcc_attr attributes;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef wcc_attr *pre_op_attr;
#endif /* !RFC_SYNTAX */

struct wcc_data {
	pre_op_attr before;
	post_op_attr after;
};

#if RFC_SYNTAX
union post_op_fh3 switch (bool present) {
case TRUE:
	nfs_fh3 handle;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef nfs_fh3 *post_op_fh3;
#endif /* !RFC_SYNTAX */

#if RFC_SYNTAX
union set_uint32 switch (bool set) {
case TRUE:
	uint32 val;
default:
	void;
};
#else /* !RFC_SYNTAX */
typedef uint32 *set_uint32;
#endif /* !RFC_SYNTAX */

#if RFC_SYNTAX
union set_uint64 switch (bool set) {
case TRUE:
	uint64 val;
default:
	void;
};
#else /* !RFC_SYNTAX */
typedef uint64 *set_uint64;
#endif /* !RFC_SYNTAX */

enum time_how {
	DONT_CHANGE = 0,
	SET_TO_SERVER_TIME = 1,
	SET_TO_CLIENT_TIME = 2
};

union set_time switch (time_how set) {
case SET_TO_CLIENT_TIME:
	nfstime3 time;
default:
	void;
};

struct sattr3 {
	set_uint32 mode;
	set_uint32 uid;
	set_uint32 gid;
	set_uint64 size;
	set_time atime;
	set_time mtime;
};

struct diropargs3 {
	nfs_fh3 dir;
	filename3 name;
};

struct diropres3ok {
	post_op_fh3 obj;
	post_op_attr obj_attributes;
	wcc_data dir_wcc;
};

union diropres3 switch (nfsstat3 status) {
case NFS3_OK:
	diropres3ok resok;
default:
	wcc_data resfail;	/* Directory attributes  */
};

union wccstat3 switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	wcc_data wcc;
};

union getattr3res switch (nfsstat3 status) {
case NFS3_OK:
	fattr3 attributes;
default:
	void;
};

#if RFC_SYNTAX
union sattrguard3 switch (bool check) {
case TRUE:
	nfstime3 ctime;
case FALSE:
	void;
};
#else /* !RFC_SYNTAX */
typedef nfstime3 *sattrguard3;
#endif /* !RFC_SYNTAX */

struct setattr3args {
	nfs_fh3 object;
	sattr3 new_attributes;
	sattrguard3 guard;
};

struct lookup3resok {
	nfs_fh3 object;
	post_op_attr obj_attributes;
	post_op_attr dir_attributes;
};

union lookup3res switch (nfsstat3 status) {
case NFS3_OK:
	lookup3resok resok;
default:
	post_op_attr resfail;	/* Directory attributes */
};

const ACCESS3_READ    = 0x0001;
const ACCESS3_LOOKUP  = 0x0002;
const ACCESS3_MODIFY  = 0x0004;
const ACCESS3_EXTEND  = 0x0008;
const ACCESS3_DELETE  = 0x0010;
const ACCESS3_EXECUTE = 0x0020;

struct access3args {
	nfs_fh3 object;
	uint32 access;
};

struct access3resok {
	post_op_attr obj_attributes;
	uint32 access;
};

union access3res switch (nfsstat3 status) {
case NFS3_OK:
	access3resok resok;
default:
	post_op_attr resfail;
};

struct readlink3resok {
	post_op_attr symlink_attributes;
	nfspath3 data;
};

union readlink3res switch (nfsstat3 status) {
case NFS3_OK:
	readlink3resok resok;
default:
	post_op_attr resfail;
};

struct read3args {
	nfs_fh3 file;
	uint64 offset;
	uint32 count;
};

struct read3resok {
	post_op_attr file_attributes;
	uint32 count;
	bool eof;
	opaque data<>;
};

union read3res switch (nfsstat3 status) {
case NFS3_OK:
	read3resok resok;
default:
	post_op_attr resfail;
};

enum stable_how {
	UNSTABLE = 0,
	DATA_SYNC = 1,
	FILE_SYNC = 2
};

struct write3args {
	nfs_fh3 file;
	uint64 offset;
	uint32 count;
	stable_how stable;
	opaque data<>;
};

struct write3resok {
	wcc_data file_wcc;
	uint32 count;
	stable_how committed;
	writeverf3 verf;
};

union write3res switch (nfsstat3 status) {
case NFS3_OK:
	write3resok resok;
default:
	wcc_data resfail;
};

enum createmode3 {
	UNCHECKED = 0,
	GUARDED = 1,
	EXCLUSIVE = 2
};

union createhow3 switch (createmode3 mode) {
case UNCHECKED:
case GUARDED:
	sattr3 obj_attributes;
case EXCLUSIVE:
	createverf3 verf;
};

struct create3args {
	diropargs3 where;
	createhow3 how;
};

struct mkdir3args {
	diropargs3 where;
	sattr3 attributes;
};

struct symlinkdata3 {
	sattr3 symlink_attributes;
	nfspath3 symlink_data;
};

struct symlink3args {
	diropargs3 where;
	symlinkdata3 symlink;
};

struct devicedata3 {
	sattr3 dev_attributes;
	specdata3 spec;
};

union mknoddata3 switch (ftype3 type) {
case NF3CHR:
case NF3BLK:
	devicedata3 device;
case NF3SOCK:
case NF3FIFO:
	sattr3 pipe_attributes;
default:
	void;
};

struct mknod3args {
	diropargs3 where;
	mknoddata3 what;
};

struct rename3args {
	diropargs3 from;
	diropargs3 to;
};

struct rename3wcc {
	wcc_data fromdir_wcc;
	wcc_data todir_wcc;
};

union rename3res switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	rename3wcc res;
};

struct link3args {
	nfs_fh3 file;
	diropargs3 link;
};

struct link3wcc {
	post_op_attr file_attributes;
	wcc_data linkdir_wcc;
};

union link3res switch (nfsstat3 status) {
#ifndef UNION_ONLY_DEFAULT
case -1:
	void;
#endif /* !UNION_ONLY_DEFAULT */
default:
	link3wcc res;
};

struct readdir3args {
	nfs_fh3 dir;
	uint64 cookie;
	cookieverf3 cookieverf;
	uint32 count;
};

struct entry3 {
	uint64 fileid;
	filename3 name;
	uint64 cookie;
	entry3 *nextentry;
};

struct dirlist3 {
	entry3 *entries;
	bool eof;
};

struct readdir3resok {
	post_op_attr dir_attributes;
	cookieverf3 cookieverf;
	dirlist3 reply;
};

union readdir3res switch (nfsstat3 status) {
case NFS3_OK:
	readdir3resok resok;
default:
	post_op_attr resfail;
};

struct readdirplus3args {
	nfs_fh3 dir;
	uint64 cookie;
	cookieverf3 cookieverf;
	uint32 dircount;
	uint32 maxcount;
};

struct entryplus3 {
	uint64 fileid;
	filename3 name;
	uint64 cookie;
	post_op_attr name_attributes;
	post_op_fh3 name_handle;
	entryplus3 *nextentry;
};

struct dirlistplus3 {
	entryplus3 *entries;
	bool eof;
};

struct readdirplus3resok {
	post_op_attr dir_attributes;
	cookieverf3 cookieverf;
	dirlistplus3 reply;
};

union readdirplus3res switch (nfsstat3 status) {
case NFS3_OK:
	readdirplus3resok resok;
default:
	post_op_attr resfail;
};

struct fsstat3resok {
	post_op_attr obj_attributes;
	uint64 tbytes;
	uint64 fbytes;
	uint64 abytes;
	uint64 tfiles;
	uint64 ffiles;
	uint64 afiles;
	uint32 invarsec;
};

union fsstat3res switch (nfsstat3 status) {
case NFS3_OK:
	fsstat3resok resok;
default:
	post_op_attr resfail;
};

const FSF3_LINK        = 0x0001;
const FSF3_SYMLINK     = 0x0002;
const FSF3_HOMOGENEOUS = 0x0008;
const FSF3_CANSETTIME  = 0x0010;

struct fsinfo3resok {
	post_op_attr obj_attributes;
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

union fsinfo3res switch (nfsstat3 status) {
case NFS3_OK:
	fsinfo3resok resok;
default:
	post_op_attr resfail;
};

struct pathconf3resok {
	post_op_attr obj_attributes;
	uint32 linkmax;
	uint32 name_max;
	bool no_trunc;
	bool chown_restricted;
	bool case_insensitive;
	bool case_preserving;
};

union pathconf3res switch (nfsstat3 status) {
case NFS3_OK:
	pathconf3resok resok;
default:
	post_op_attr resfail;
};

struct commit3args {
	nfs_fh3 file;
	uint64 offset;
	uint32 count;
};

struct commit3resok {
	wcc_data file_wcc;
	writeverf3 verf;
};

union commit3res switch (nfsstat3 status) {
case NFS3_OK:
	commit3resok resok;
default:
	wcc_data resfail;
};

program NFS_PROGRAM {
	version NFS_V3 {
		void
		NFSPROC3_NULL (void) = 0;
		
		getattr3res
		NFSPROC3_GETATTR (nfs_fh3) = 1;
		
		wccstat3
		NFSPROC3_SETATTR (setattr3args) = 2;
		
		lookup3res
		NFSPROC3_LOOKUP (diropargs3) = 3;
		
		access3res
		NFSPROC3_ACCESS (access3args) = 4;
		
		readlink3res
		NFSPROC3_READLINK (nfs_fh3) = 5;
		
		read3res
		NFSPROC3_READ (read3args) = 6;
		
		write3res
		NFSPROC3_WRITE (write3args) = 7;
		
		diropres3
		NFSPROC3_CREATE (create3args) = 8;
		
		diropres3
		NFSPROC3_MKDIR (mkdir3args) = 9;
		
		diropres3
		NFSPROC3_SYMLINK (symlink3args) = 10;
		
		diropres3
		NFSPROC3_MKNOD (mknod3args) = 11;
		
		wccstat3
		NFSPROC3_REMOVE (diropargs3) = 12;
		
		wccstat3
		NFSPROC3_RMDIR (diropargs3) = 13;
		
		rename3res
		NFSPROC3_RENAME (rename3args) = 14;
		
		link3res
		NFSPROC3_LINK (link3args) = 15;
		
		readdir3res
		NFSPROC3_READDIR (readdir3args) = 16;
		
		readdirplus3res
		NFSPROC3_READDIRPLUS (readdirplus3args) = 17;
		
		fsstat3res
		NFSPROC3_FSSTAT (nfs_fh3) = 18;
		
		fsinfo3res
		NFSPROC3_FSINFO (nfs_fh3) = 19;
		
		pathconf3res
		NFSPROC3_PATHCONF (nfs_fh3) = 20;
		
		commit3res
		NFSPROC3_COMMIT (commit3args) = 21;
	} = 3;
} = 100003;
