
/*
 * This file was written by Frans Kaashoek and Kevin Fu.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"
%#include "sfs_prot.h"

const SFSRO_FHSIZE = 20;
const SFSRO_BLKSIZE = 8192;
const SFSRO_NFH = 256;	       /* Blocks are approx 2KB each */
const SFSRO_NDIR = 7;
const SFSRO_FHDB_KEYS     = 255;  
const SFSRO_FHDB_CHILDREN = 256; /* must be  KEYS+1 */
const SFSRO_FHDB_NFH      = 256; /* FHDB blocks are approx 5KB each */

enum sfsrostat {
  SFSRO_OK = 0,
  SFSRO_ERRNOENT = 1
};

struct sfsro_dataresok {
  unsigned offset;
  unsigned size; 
  opaque data<>;
};

union sfsro_datares switch (sfsrostat status) {
 case SFSRO_OK:
   sfsro_dataresok resok;
 default: 
   void;
};

struct sfsro_getarg {
  sfs_hash fh;
  unsigned offset;
  unsigned len;
};

enum ftypero {
  SFSROREG      = 1,
  SFSROREG_EXEC = 2,  /* Regular, executable file */
  SFSRODIR      = 3, 
  SFSRODIR_OPAQ = 4,
  SFSROLNK      = 5
};


struct sfsro_inode_lnk {
  uint32 nlink;
  nfstime3 mtime;
  nfstime3 ctime;

  nfspath3 dest;
};

struct sfsro_inode_reg {
  uint32 nlink;
  uint64 size;
  uint64 used; 
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
  string name<>;
  sfsro_dirent *nextentry;
  /*  uint64 fileid; */
};

struct sfsro_directory {
  nfspath3 path;
/*  uint64 fileid; */
  sfsro_dirent *entries;
  bool eof;
};


struct sfsro_fhdb_indir {
  /*
     Invariant:
                key[i] < key [j] for all i<j

                keys in GETDATA(child[i]) are 
                   <= key[i+1] <
                keys in GETDATA(child[i+1])
  */
  sfs_hash key<SFSRO_FHDB_KEYS>;     
  sfs_hash child<SFSRO_FHDB_CHILDREN>;
};

/* Handles to direct blocks */
typedef sfs_hash sfsro_fhdb_dir<SFSRO_FHDB_NFH>;


enum dtype {
   SFSRO_INODE      = 0,
   SFSRO_FILEBLK    = 1, /* File data */
   SFSRO_DIRBLK     = 2, /* Directory data */
   SFSRO_INDIR      = 3, /* Indirect data pointer block */
   SFSRO_FHDB_DIR   = 4, /* Direct data pointer block for FH database */
   SFSRO_FHDB_INDIR = 5  /* Indirect data pointer block for FH database */
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
 case SFSRO_FHDB_DIR:
   sfsro_fhdb_dir fhdb_dir;
 case SFSRO_FHDB_INDIR:
   sfsro_fhdb_indir fhdb_indir;
 default:
   void;
};


program SFSRO_PROGRAM {
	version SFSRO_VERSION {
		void 
		SFSROPROC_NULL (void) = 0;

		sfsro_datares
		SFSROPROC_GETDATA (sfsro_getarg) = 1;

	} = 1;
} = 344446;
