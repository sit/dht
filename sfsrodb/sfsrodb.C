/* $Id: sfsrodb.C,v 1.8 2001/03/26 08:37:10 fdabek Exp $ */

/*
 * Copyright (C) 1999 Kevin Fu (fubob@mit.edu)
 * Copyright (C) 1999 Frans Kaashoek (kaashoek@mit.edu)
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
 * Foundation, Inc.,4 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

/*
 * This program will generate the integrity database for the SFS read-only
 * file system.  Run this program after every change to exported files 
 */


/* The hash tree works very similar to that of the indirect data pointers
   in an inode. */

#include "sysconf.h"

#include <dirent.h>
#include <unistd.h>
#include <stdlib.h>
#include "sfsrodb.h"
#include "parseopt.h"
#include "vec.h"
#include "arpc.h"
#include "dhash.h"
#include "dhash_prot.h"

#define MIN(a,b) (((a)<(b))?(a):(b))

ptr<aclnt> sfsrodb;

str root2;

bool initialize;
bool verbose_mode;
u_int32_t blocksize;
extern int errno;
char IV[SFSRO_IVSIZE];
int relpathlen;
extern ptr < rabin_priv > sfsrokey;
str hostname;


/* Statistics */
u_int32_t reginode_cnt = 0;
u_int32_t lnkinode_cnt = 0;
u_int32_t filedatablk_cnt = 0;
u_int32_t sindir_cnt = 0;
u_int32_t dindir_cnt = 0;
u_int32_t tindir_cnt = 0;
u_int32_t directory_cnt = 0;
//int32_t fhdb_cnt = 0;
u_int32_t fh_cnt = 0;

u_int32_t identical_block = 0;
u_int32_t identical_sindir = 0;
u_int32_t identical_dindir = 0;
u_int32_t identical_tindir = 0;
u_int32_t identical_dir = 0;
u_int32_t identical_inode = 0;
u_int32_t identical_sym = 0;
//u_int32_t identical_fhdb = 0;
u_int32_t identical_fh = 0;

time_t sfsro_duration = 86400; /* default to 1 day */


/* True if only can LOOKUP, not READDIR 
   Really should make more fine grained.
   Allow specification of which directories to 
   make opaque.
*/
bool opaque_directory = false; 


/* Given: A filled stat structure and allocated inode
   Return: A SFSRO inode which reflects all of the stat values.
   The data pointers are initialized with .setsize(0).
   The array of direct pointers is initialize with no members.
   However, the caller will eventually have to set the
   following values: 

   .size, .used, and direct/indirect data pointers.   
 */
void
sfsrodb_setinode (const struct stat *st, sfsro_inode *inode)
{

  /*
     SFSRO has implied read-access by all.  We only care
     whether the file is a non-directory executable.  Everything
     else is synthesised by sfsrocd.

   */

  ftypero t;
  if (S_ISREG (st->st_mode)) {
    t = ((st->st_mode & (S_IXUSR | S_IXGRP | S_IXOTH)) ?
		 SFSROREG_EXEC : SFSROREG);
  }
  else if (S_ISDIR (st->st_mode))
    t = opaque_directory ? SFSRODIR_OPAQ : SFSRODIR;
  else if (S_ISLNK (st->st_mode))
    t = SFSROLNK;
  else {
    warn << "Non-supported file type " << st->st_mode << "\n";
    exit (1);
  }

  inode->set_type (t);

  if (inode->type == SFSROLNK) {
    rpc_clear (*inode->lnk);

    inode->lnk->nlink = st->st_nlink; // XXX bogus! cannot rely on this number

#ifdef SFS_HAVE_STAT_ST_ATIMESPEC
    inode->lnk->mtime.seconds = st->st_mtimespec.tv_sec;
    inode->lnk->mtime.nseconds = st->st_mtimespec.tv_nsec;
    inode->lnk->ctime.seconds = st->st_ctimespec.tv_sec;
    inode->lnk->ctime.nseconds = st->st_ctimespec.tv_nsec;
#else
    inode->lnk->mtime.seconds = st->st_mtime;
    inode->lnk->mtime.nseconds = 0;
    inode->lnk->ctime.seconds = st->st_ctime;
    inode->lnk->ctime.nseconds = 0;
#endif /* SFS_HAVE_ST_ATIMESPEC */

  } else {
    rpc_clear (*inode->reg);

    inode->reg->nlink = st->st_nlink;  // XXX bogus! cannot rely on this number
    inode->reg->size = 0;
    inode->reg->used = 0;
    
#ifdef SFS_HAVE_STAT_ST_ATIMESPEC
    inode->reg->mtime.seconds = st->st_mtimespec.tv_sec;
    inode->reg->mtime.nseconds = st->st_mtimespec.tv_nsec;
    inode->reg->ctime.seconds = st->st_ctimespec.tv_sec;
    inode->reg->ctime.nseconds = st->st_ctimespec.tv_nsec;
#else
    inode->reg->mtime.seconds = st->st_mtime;
    inode->reg->mtime.nseconds = 0;
    inode->reg->ctime.seconds = st->st_ctime;
    inode->reg->ctime.nseconds = 0;
#endif /* SFS_HAVE_ST_ATIMESPEC */

    inode->reg->direct.setsize (0);

  }

  // strbuf sb;
  // rpc_print (sb, inode, 5, NULL, NULL);
  // warn << "setinode " << sb << "\n";

}


/* Given: A fully specified inode
   Effects: Stores the inode in the database with the fh as the key
   Return: A file handle in fh.  fh must already be allocated.
   This function will set the IV appropriately with prandom bits.
 */
void
store_inode (sfsro_inode *inode, sfs_hash *fh)
{
  sfsro_data dat (SFSRO_INODE);
  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);

  *dat.inode = *inode;
  if (xdr_sfsro_data (x.xdrp (), &dat)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  create_sfsrofh (IV, SFSRO_IVSIZE, fh, callbuf, calllen);

  // Store the inode of this path in the database
  if (!sfsrodb_put (sfsrodb, fh->base (), fh->size (), callbuf, calllen)) {
    //    warn << "Found identical inode, compressing.\n";
    identical_inode++;
    identical_fh++;
  } else {
    fh_cnt++;
    if (inode->type == SFSROLNK)
      lnkinode_cnt++;
    else
      reginode_cnt++;
  }

  xfree (callbuf);
}


/* Given: A block <= 8KB, its size, an allocated fh
   Return: A new fh in fh.  Return false if fh is duplicate
 */
bool
store_file_block (sfs_hash *fh, const char *block, size_t size)
{
  sfsro_data res (SFSRO_FILEBLK);
  res.data->setsize (size);
  memcpy (res.data->base (), block, size);

  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);

  if (xdr_sfsro_data (x.xdrp (), &res)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  create_sfsrofh (IV, SFSRO_IVSIZE, fh, callbuf, calllen);

  if (!sfsrodb_put (sfsrodb, fh->base (), fh->size (), callbuf, calllen)) {
    //    warnx << "Found identical block, compressing\n"; 
    identical_block++;
    identical_fh++;

    xfree (callbuf);
    return false;
  }

  filedatablk_cnt++;
  fh_cnt++;
      
  xfree (callbuf);
  return true;
}

inline bool
process_sindirect (int &fd, bool &wrote_stuff, sfsro_inode *inode,
		   char *block, sfs_hash &block_fh, sfs_hash *fh) {

  size_t size = 0;

  //  warnx << "Adding sindirect pointers\n"; 
  uint32 blocknum = 0;
  sfsro_data sindir (SFSRO_INDIR);
  sindir.indir->handles.setsize (SFSRO_NFH);
  
  while (blocknum < SFSRO_NFH) {
    
    size = read (fd, block, SFSRO_BLKSIZE);

    if (size <= 0)
      break;

    inode->reg->size += size;
    
    /* Check for identical blocks */
    if (store_file_block (&block_fh, block, size)) {
      inode->reg->used += size;
      
      //	warnx << "Added direct, size " << size << ", blocknum " 
      //    << blocknum << "\n";
    }
      
    sindir.indir->handles[blocknum] = block_fh;
    blocknum++;      
  }
    
  if (size < 0) {
    warnx << "store_file: Read failed in sindirect pointers\n";
    exit (1);
  }

  if (blocknum != 0) {
    size_t calllen = 0;
    char *callbuf = NULL;
    xdrsuio x (XDR_ENCODE);
    if (xdr_sfsro_data (x.xdrp(), &sindir)) {
      calllen = x.uio ()->resid ();
      callbuf = suio_flatten (x.uio ());
    }
    
    create_sfsrofh (IV, SFSRO_IVSIZE, fh, 
		    callbuf, calllen);

    if (!sfsrodb_put (sfsrodb, fh->base(), fh->size(),
		      callbuf, calllen)) {
      // warn << "Found identical sindirect, compressing.\n";
      identical_sindir++;
      identical_fh++;
    } else {
      sindir_cnt++;
      fh_cnt++;
    }
    xfree (callbuf);
    wrote_stuff = true;
  } else {
    wrote_stuff = false;
  }
    
  return (size == 0);
}


inline bool
process_dindirect (int &fd, bool &wrote_stuff, sfsro_inode *inode, 
		   char *block, sfs_hash &block_fh, sfs_hash *fh) {
  
  bool done = false;

  //  warnx << "Adding dindirect pointers\n"; 
  uint32 blocknum = 0;
  sfsro_data dindir (SFSRO_INDIR);
  dindir.indir->handles.setsize (SFSRO_NFH);
  // XXX we could be smarter here by only allocating
  // the number of file handles we actually need in the dindirect.

  while (!done && blocknum < SFSRO_NFH) {

    done = process_sindirect (fd, wrote_stuff, inode, block, block_fh, 
			      &dindir.indir->handles[blocknum]);

    if (!wrote_stuff)
      break;
	
    //    warnx << "Added ddirect, blocknum " 
    //  << blocknum << "\n";
    
    blocknum++;      
  }
  
  if (blocknum != 0) {
    size_t calllen = 0;
    char *callbuf = NULL;
    xdrsuio x (XDR_ENCODE);
    if (xdr_sfsro_data (x.xdrp(), &dindir)) {
      calllen = x.uio ()->resid ();
      callbuf = suio_flatten (x.uio ());
    }
    
    create_sfsrofh (IV, SFSRO_IVSIZE, fh, 
		    callbuf, calllen);

    if (!sfsrodb_put (sfsrodb, fh->base(), fh->size(),
		      callbuf, calllen)) {
      // warn << "Found identical dindirect, compressing.\n";
      identical_dindir++;
      identical_fh++;
    } else {
      dindir_cnt++;
      fh_cnt++;
    }
    
    xfree (callbuf);
    wrote_stuff = true;
  } else {
    wrote_stuff = false;
  }
  
  return done;
}

inline bool
process_tindirect (int &fd, bool &wrote_stuff, sfsro_inode *inode, 
		   char *block, sfs_hash &block_fh, sfs_hash *fh) {
  
  bool done = false;

  //  warnx << "Adding tindirect pointers\n"; 
  uint32 blocknum = 0;
  sfsro_data tindir (SFSRO_INDIR);
  tindir.indir->handles.setsize (SFSRO_NFH);
  // XXX we could be smarter here by only allocating
  // the number of file handles we actually need in the tindirect.
  
  while (!done && blocknum < SFSRO_NFH) {
    
    done = process_dindirect (fd, wrote_stuff, inode, block, block_fh,
			      &tindir.indir->handles[blocknum]);

    if (!wrote_stuff)
      break;
    
    //    warnx << "Added tdirect, blocknum " 
    //	  << blocknum << "\n";
      
    blocknum++;      
  }

  if (blocknum != 0) {
    size_t calllen = 0;
    char *callbuf = NULL;
    xdrsuio x (XDR_ENCODE);
    if (xdr_sfsro_data (x.xdrp(), &tindir)) {
      calllen = x.uio ()->resid ();
      callbuf = suio_flatten (x.uio ());
    }
    
    create_sfsrofh (IV, SFSRO_IVSIZE, fh, 
		    callbuf, calllen);

    if (!sfsrodb_put (sfsrodb, fh->base(),
		      fh->size(),
		      callbuf, calllen)) {
      // warn << "Found identical tindirect, compressing.\n";
      identical_tindir++;
      identical_fh++;
    } else {
      tindir_cnt++;
      fh_cnt++;
    }
    
    xfree (callbuf);
    wrote_stuff = true;
  } else {
    wrote_stuff = false;
  }

  return done;
}

/* Given: A fully specified inode for a file and pointer to its data
   (but not file sizes or data pointers)
   Effects: Store the file data, fully specify the inode
   
   Return: A file handle in fh.  fh must already be allocated.
   This function will set the IV appropriately with prandom bits.
 */
void
store_file (sfsro_inode *inode, str path)
{
  char block[SFSRO_BLKSIZE];
  sfs_hash block_fh;
  int fd;
  size_t size = 0;
  bool done = false;

  if ((fd = open (path, O_RDONLY)) < 0) {
    warn << "store_file: open failed" << fd << "\n";
    exit (1);
  }


  // Deal with direct pointers
  uint32 blocknum = 0;
  inode->reg->direct.setsize (SFSRO_NDIR);
  bzero (inode->reg->direct.base (), SFSRO_NDIR * sizeof (sfs_hash));
  
  while (blocknum < SFSRO_NDIR) {      
    
    size = read (fd, block, SFSRO_BLKSIZE);

    if (size <= 0)
      break;

    inode->reg->size += size;

    /* Check for identical blocks */
    if (store_file_block (&block_fh, block, size)) {
      inode->reg->used += size;
      //      warnx << "Added direct, size " << size << ", blocknum " 
      //    << blocknum << "\n";
    }

    inode->reg->direct[blocknum] = block_fh;
    blocknum++;
  }

  if (size < 0) {
    warnx << "store_file: Read failed in direct pointers\n";
    exit (1);
  } else if (size != 0) {

    bool wrote_stuff = false;

    // Deal with sindirect pointers
    done = process_sindirect (fd, wrote_stuff, inode, 
			      block, block_fh,
			      &inode->reg->indirect);
    
    // Deal with dindirect pointers
    if (!done) {
      done = process_dindirect (fd, wrote_stuff, inode, 
				block, block_fh,
				&inode->reg->double_indirect);
      
      // Deal with tindirect pointers
      if (!done)
	done = process_tindirect (fd, wrote_stuff, inode, 
				  block, block_fh,
				  &inode->reg->triple_indirect);
    }
  }
  
  if (close (fd) < 0) {
    warn << "store_file: close failed\n";
    exit (1);
  }


}

/* Given: a fully specified directory, inode filled in by setinode,
   allocated fh

   Return: file handle, store directory contents, final inode values

   Effects: After filling in a directory structure, fill in an inode
   structure, store the directory in the database, and compute file
   handle.  
 */
void
store_directory (sfsro_inode *inode, sfs_hash *fh,
		 sfsro_data *directory)
{
  // XXX???  sfsro_data dat (SFSRO_DIRBLK);
  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);

  if (xdr_sfsro_data (x.xdrp (), directory)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  create_sfsrofh (IV, SFSRO_IVSIZE, fh, callbuf, calllen);

  if (!sfsrodb_put (sfsrodb, fh->base (), fh->size (), callbuf, calllen)) {
    //warn << "Found identical directory, compressing.\n";
    identical_dir++;
    identical_fh++;
  } else {
    directory_cnt++;
    fh_cnt++;
  }


  xfree (callbuf);

  // This is bogus, we're just using one data pointer regardless
  // of the size of the directory.  In a correct implementation,
  // we should only store 8KB per data block
  inode->reg->size = inode->reg->used = calllen;
  inode->reg->direct.setsize (1);
  inode->reg->direct[0] = *fh;

  //  strbuf sb;
  //rpc_print (sb, *inode, 5, NULL, " ");
  //warn << "storedirectory " << sb << "\n";


  return;
}


static int
compare_name (const void *file1, const void *file2)
{
  return (strcmp (*(static_cast < char **>(file1)),
		  *(static_cast < char **>(file2))));
}


void
sort_dir (const str path, vec < char *>&file_list)
{
  DIR *dirp;
  struct dirent *de = NULL;
  char *filename;

  if ((dirp = opendir (path)) == 0) {
    warn << path << " is not a directory\n";
    return;
  }

  while ((de = readdir (dirp))) {
    filename = New char[strlen (de->d_name) + 1];
    memcpy (filename, de->d_name, strlen (de->d_name) + 1);
    file_list.push_back (filename);
  }

  /*
  warnx << "Before:\n";

  for (unsigned int i = 0; i < file_list.size (); i++) {
       warnx << file_list[i] << "\n";
  }
  */

  qsort (file_list.base (), file_list.size (),
	 sizeof (char *), compare_name);

  /*
  warnx << "After:\n";

  for (unsigned int i = 0; i < file_list.size (); i++) {
    warnx << file_list[i] << "\n";
  }
  */
}

/* pseudo-destructor */
void
twiddle_sort_dir (vec < char *>&file_list)
{
  for (unsigned int i = 0; i < file_list.size (); i++) {
    delete file_list[i];
  }
}

/*
   Given: Path to file (any kind), an allocated fh, the inode
   number of the path's parent

   Return: The hash and IV in the fh.

   Effects: Recursively hash everything beneath a directory
   It computes the cryptographic file handle for the
   given path, inserts the mapping from the fh to its data

 */
int
recurse_path (const str path, sfs_hash * fh)
{
  struct stat st;

  //  warn << "recurse_path (" << path << ", " << "fh)\n";

  if (lstat (path, &st) < 0)
    return -1;

  sfsro_inode inode;

  if (S_ISLNK (st.st_mode)) {
    
    char *buf = New char[st.st_size + 1];
    int nchars = readlink (path, buf, st.st_size);

    if (nchars > MAXPATHLEN) {
      warn << "symlink target too large "
	<< path << " " << nchars << "\n";
      return -1;
    }

    //    buf[nchars] = 0;

    //    warn << "recurse_path: Link\n";

    sfsrodb_setinode (&st, &inode);
    inode.lnk->dest = nfspath3 (buf, nchars);

    delete[] buf;


  } else if (S_ISREG (st.st_mode)) {
    //    warn << "recurse_path: Regular file\n";

    sfsrodb_setinode (&st, &inode);

    store_file (&inode, path);

    /*    strbuf sb;
       rpc_print (sb, inode, 5, NULL, " ");
       warn << "inode " << sb << "\n";
     */
  }
  else if (S_ISDIR (st.st_mode)) {
    //    warn << "recurse_path: Directory\n";

    sfsrodb_setinode (&st, &inode);

    /*    strbuf sb;
       rpc_print (sb, inode, 5, NULL, " ");
       warn << "inode " << sb << "\n";
     */

    //    sorted_dir dir (path);

    vec < char *>file_list;

    sort_dir (path, file_list);

    sfsro_data directory (SFSRO_DIRBLK);

    // XXXXX need to take of the prefix of path
    directory.dir->path = substr (path, relpathlen);

        warn << "Dir path" << directory.dir->path << "\n";

    directory.dir->eof = true;
    rpc_ptr < sfsro_dirent > *direntp = &directory.dir->entries;

    for (unsigned int i = 0; i < file_list.size (); i++) {
      (*direntp).alloc ();
      (*direntp)->name = file_list[i];
      //      (*direntp)->fh.setsize (0);

      /* The client manages . and .. */
      if (((*direntp)->name.cmp (".") == 0) ||
	  ((*direntp)->name.cmp ("..") == 0)) {
	continue;
	/*      (*direntp)->fh.setsize (sizeof (uint64));
	   memcpy (static_cast<void *>((*direntp)->fh.base ()),
	   static_cast<void *>(&st.st_ino),
	   sizeof (uint64)); */
      }
      else {
	//	warn << "File: " << (*direntp)->name << "\n";

	recurse_path (path << "/" << (*direntp)->name,
		      &(*direntp)->fh);

	//	warn << "adding dirent: " << (*direntp)->name << "\n";
      }

      direntp = &(*direntp)->nextentry;
    }

    twiddle_sort_dir (file_list);

    store_directory (&inode, fh, &directory);
  }
  else {
    warn << "Not symlink, reg file, or directory " << path << "\n";
    return -1;
  }

  store_inode (&inode, fh);

  return 0;

}


int
sfsrodb_main (const str root, const str keyfile, const char *dbfile)
{

  int fd = unixsocket_connect(dbfile);
  if (fd < 0)
    fatal << "error on " << dbfile << "\n";
  
  sfsrodb = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  
  /* Set the sfs_connectres structure (with pub key) to db */
  sfs_connectres cres (SFS_OK);
  cres.reply->servinfo.release = SFS_RELEASE;
  cres.reply->servinfo.host.type = SFS_HOSTINFO;
  cres.reply->servinfo.host.hostname = hostname;

  ptr < rabin_priv > sk;

  if (!keyfile) {
    warn << "cannot locate default file sfs_host_key\n";
    fatal ("errors!\n");
  }
  else {
    str key = file2wstr (keyfile);
    warn << key << "\n\n";
    if (!key) {
      warn << keyfile << ": " << strerror (errno) << "\n";
      fatal ("errors!\n");
    }
    else if (!(sk = import_rabin_priv (key, NULL))) {
      warn << "could not decode " << keyfile << "\n";
      warn << key << "\n";
      fatal ("errors!\n");
    }
  }

  cres.reply->servinfo.host.pubkey = sk->n;
  cres.reply->servinfo.prog = SFSRO_PROGRAM;
  cres.reply->servinfo.vers = SFSRO_VERSION;
  bzero (&cres.reply->charge, sizeof (sfs_hashcharge));

  // Set IV
  sfs_hash id;
  sfs_mkhostid (&id, cres.reply->servinfo.host);
  memcpy (&IV[0], "squeamish_ossifrage_", SFSRO_IVSIZE);

  // store file system in db
  sfs_hash root_fh;
  relpathlen = root.len ();
  recurse_path (root, &root_fh);

  sfs_fsinfo res (SFSRO_PROGRAM);
  res.sfsro->set_vers (SFSRO_VERSION);

  memcpy (res.sfsro->v1->info.iv.base (), &IV[0], SFSRO_IVSIZE);
  res.sfsro->v1->info.rootfh = root_fh;

  time_t start, end;

  res.sfsro->v1->info.type = SFS_ROFSINFO;
  res.sfsro->v1->info.start = start = time (NULL);
  res.sfsro->v1->info.duration = sfsro_duration;

  end = start + sfsro_duration;

  // XX Should make sure timezone is correct
  str stime (ctime (&start));
  str etime (ctime (&end));
  warn << "Database good from: \n " << stime
       << "until:\n " << etime;

  create_sfsrosig (&(res.sfsro->v1->sig), &(res.sfsro->v1->info),
		   keyfile);

  str fsinfo_name;
  if  (initialize) 
    fsinfo_name = str("fsinfo");
  else {
    const char *pk_raw = sk->n.cstr ();
    char hashed_pk[21];
    sha1_hash(hashed_pk, pk_raw, strlen(pk_raw));
    fsinfo_name = armor32(hashed_pk, 20);
  }
  warn << "inserting fsinfo under " << fsinfo_name << "\n";
 

  xdrsuio x (XDR_ENCODE);
  if (xdr_sfs_fsinfo (x.xdrp (), &res)) {
    void *v = suio_flatten (x.uio ());
    int l =  x.uio ()->resid ();
    if (initialize)  sfsrodb_put (sfsrodb, "fsinfo", 6, v, l);
    else  sfsrodb_put (sfsrodb, fsinfo_name.cstr (), 20, v, l);

    warn << "Added fsinfo\n";
  }

  if (initialize) {
    warn << "adding cres, do not use this option with chord\n";
    xdrsuio x2 (XDR_ENCODE);
    if (xdr_sfs_connectres (x2.xdrp (), &cres)) {
      int l = x2.uio ()->resid ();
      void *v = suio_flatten (x2.uio ());
      warn << "put conres in db\n";
      if (!sfsrodb_put (sfsrodb, "conres", 6, v, l)) {
	warn << "Found identical conres. You found a collision!\n";
	exit (-1);
      }
    }
  }

  if (verbose_mode) {
    warn << "identical blocks:   " << identical_block << "\n";
    warn << "identical sindirs:  " << identical_sindir << "\n";
    warn << "identical dindirs:  " << identical_dindir << "\n";
    warn << "identical tindirs:  " << identical_tindir << "\n";
    warn << "identical dirs:     " << identical_dir << "\n";
    warn << "identical inodes:   " << identical_inode << "\n";
    warn << "identical symlinks: " << identical_sym << "\n";
    //    warn << "identical fhdb:     " << identical_fhdb << "\n\n\n";

    warn << "Database contents:\n";
    warn << "Regular inodes:      " << reginode_cnt << "\n";
    warn << "Symlink inodes:      " << lnkinode_cnt << "\n";
    warn << "Directory blocks     " << directory_cnt << "\n";
    warn << "File data blocks:    " << filedatablk_cnt << "\n";
    warn << "Single indir blocks: " << sindir_cnt << "\n";
    warn << "Double indir blocks: " << dindir_cnt << "\n";
    warn << "Triple indir blocks: " << tindir_cnt << "\n";
    //    warn << "Fhdb blocks:         " << fhdb_cnt << "\n\n\n";

    warn << "identical fh's overall : " << identical_fh << "\n";
    warn << "unique fh's overall    : " << fh_cnt << "\n\n\n";
  }

  warn << "close db\n";

  //  sfsrodb->closedb ();

  //  delete sfsrodb;

  // XXX here we should verify for debugging that the number of entries
  // in fhdb is the same as the number of database entries (minus
  // the conres and root)

  return 0;
}


static void
usage ()
{
  warnx << "usage: " << progname
	<< " -d <export directory> -s <SK keyfile> -o <dbfile>\n";
  warnx << "              [-i] [-h <hostname for db>] [-v] [-b <blocksize>]\n";
  warnx << "-d <export directory> : The directory hierarchy to export\n";
  warnx << "-s <SK keyfile>       : Path to the secret key file\n";
  warnx << "-o <dbfile>           : Filename to output database\n";
  warnx << "Optional directives:\n";
  warnx << "-i                    : Initialize local DB (for testing only)\n";
  warnx << "-h <hostname for db>  : Hostname of replication, if not this machine\n";
  warnx << "-v                    : Verbose debugging output\n";
  warnx << "-b <blocksize>        : Page size of underlying database\n";


  //  warnx << "usage: " << progname << " [command] [options]\n\n";
  //warnx << "\tinit directory [-d sdb file] [-followsymlinks] [-maxdepth max] [-key keyfile]\n";
  //  warnx << "\tupdate directory \n";
  exit (1);
}


int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  char h[1024];
  strcpy(h, myname ().cstr());
  for (unsigned int i = 0; i < strlen(h); i++) h[i] = tolower(h[i]);
  
  hostname = str (h);

  char *exp_dir = NULL;
  char *sk_file = NULL;
  char *output_file = NULL;

  verbose_mode = false;
  initialize = false;

  int ch;
  while ((ch = getopt (argc, argv, "b:d:e:s:o:h:vi")) != -1)
    switch (ch) {
    case 'b':
      if (!convertint (optarg, &blocksize)
	  || blocksize < 512 || blocksize > 0x10000)
	usage ();
      break;
    case 'd':
      exp_dir = optarg;
      break;
    case 'e':
      root2 = optarg;
      break;
    case 's':
      sk_file = optarg;
      break;
    case 'o':
      output_file = optarg;
      break;
    case 'h':
      hostname = optarg;
      break;
    case 'i':
      initialize = true;
      break;
    case 'v':
      verbose_mode = true;
      break;
    case '?':
    default:
      usage ();
    }
  argc -= optind;
  argv += optind;

  if ( (argc > 0) || !exp_dir || !sk_file || !output_file )
    usage ();

  if (verbose_mode) {
    warnx << "export directory : " << exp_dir << "\n";
    warnx << "SK keyfile       : " << sk_file << "\n";
    warnx << "dbfile           : " << output_file << "\n";
    warnx << "Initialize mode : ";
    if (initialize) 
      warnx << "On\n";
    else
      warnx << "Off\n";

    warnx << "hostname for db  : " << hostname << "\n";
  }

  return (sfsrodb_main (exp_dir, sk_file, output_file));
}
