
/*
 * This file was written by Frans Kaashoek and Kevin Fu.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"
%#include "sfs_prot.h"
%#include "chord.h"

const SFSRO_FHSIZE = 20;
const SFSRO_NDIR = 128;

enum ftypero {
  SFSROREG      = 1,
  SFSROREG_EXEC = 2,  /* Regular, executable file */
  SFSRODIR      = 3, 
  SFSRODIR_OPAQ = 4,
  SFSROLNK      = 5
};


struct cfs_fsinfo {
  unsigned start;       /* In seconds since UNIX epoch */
  unsigned duration;	/* seconds */
  chordID rootfh;
  unsigned blocksize;
};

/* XXX shouldn't all fields common to sfsro_inode_lnk and sfs_inode_reg
 *     be stuck in sfs_inode?
 *     --josh
 */

struct sfsro_inode_lnk {
  nfspath3 path;
  uint32 nlink;
  nfstime3 mtime;
  nfstime3 ctime;
  nfspath3 dest;
};

struct sfsro_inode_reg {
  nfspath3 path;
  uint32 nlink;
  uint64 size;
  uint64 used;  /* XXX this field appears to be useless --josh */ 
  nfstime3 mtime;
  nfstime3 ctime;
 
  sfs_hash direct<SFSRO_NDIR>;
  sfs_hash indirect;
  sfs_hash double_indirect;
  sfs_hash triple_indirect;

};


union sfsro_inode switch (ftypero type) {
case SFSROLNK:
  sfsro_inode_lnk lnk;
default:
  sfsro_inode_reg reg;
};


struct sfsro_indirect {
  sfs_hash handles<>;
};

struct sfsro_dirent {
  sfs_hash fh;
  uint64 fileid;
  string name<>;
  sfsro_dirent *nextentry;
};

struct sfsro_directory {
  sfsro_dirent *entries;
  bool eof;
};

enum dtype {
   SFSRO_INODE      = 0,
   SFSRO_FILEBLK    = 1, /* File data */
   SFSRO_DIRBLK     = 2, /* Directory data */
   SFSRO_INDIR      = 3, /* Indirect data pointer block */
   CFS_FSINFO       = 4  /* root of Meta-data for file system */
};

union sfsro_data switch (dtype type) {
 case SFSRO_INODE:
   sfsro_inode inode;
 case SFSRO_FILEBLK:
   opaque data<>;
 case SFSRO_DIRBLK:
   sfsro_directory dir;
 case SFSRO_INDIR:
   sfsro_indirect indir;
 case CFS_FSINFO:
   cfs_fsinfo fsinfo;
 default:
   void;
};


