
/*
 * This file was written by Frans Kaashoek and Kevin Fu.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"
%#include "sfs_prot.h"
%#include "chord.h"

const STARFS_FHSIZE = 20;
const STARFS_NDIR = 128;

enum ftypero {
  STARFSREG      = 1,
  STARFSREG_EXEC = 2,  /* Regular, executable file */
  STARFSDIR      = 3, 
  STARFSDIR_OPAQ = 4,
  STARFSLNK      = 5
};


struct starfs_fsinfo {
  bigint rootfh;
  unsigned blocksize;
  unsigned replicas; /* number of replicas the root block has */
};

/* XXX shouldn't all fields common to starfs_inode_lnk and sfs_inode_reg
 *     be stuck in sfs_inode?
 *     --josh
 */

struct starfs_inode_lnk {
  nfspath3 path;
  uint32 nlink;
  nfstime3 mtime;
  nfstime3 ctime;
  nfspath3 dest;
};

struct starfs_inode_reg {
  nfspath3 path;
  uint32 nlink;
  uint64 size;
  uint64 used;  /* XXX this field appears to be useless --josh */ 
  nfstime3 mtime;
  nfstime3 ctime;
 
  chordID direct<STARFS_NDIR>;
  chordID indirect;
  chordID double_indirect;
  chordID triple_indirect;

};


union starfs_inode switch (ftypero type) {
case STARFSLNK:
  starfs_inode_lnk lnk;
default:
  starfs_inode_reg reg;
};


struct starfs_indirect {
  chordID handles<>;
};

struct starfs_dirent {
  chordID fh;
  uint64 fileid;
  string name<>;
  starfs_dirent *nextentry;
};

struct starfs_directory {
  starfs_dirent *entries;
  bool eof;
};

enum dtype {
   STARFS_INODE      = 0,
   STARFS_FILEBLK    = 1, /* File data */
   STARFS_DIRBLK     = 2, /* Directory data */
   STARFS_INDIR      = 3, /* Indirect data pointer block */
   STARFS_FSINFO       = 4  /* root of Meta-data for file system */
};

union starfs_data switch (dtype type) {
 case STARFS_INODE:
   starfs_inode inode;
 case STARFS_FILEBLK:
   opaque data<>;
 case STARFS_DIRBLK:
   starfs_directory dir;
 case STARFS_INDIR:
   starfs_indirect indir;
 case STARFS_FSINFO:
   starfs_fsinfo fsinfo;
 default:
   void;
};


