/* $Id: sfsrodb.C,v 1.13 2001/10/06 23:42:45 cates Exp $ */

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
#include "sha1.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define LSD_SOCKET "/tmp/chord-sock"

ptr<aclnt> cclnt;

u_int32_t blocksize;
u_int32_t nfh;

bool initialize;
bool verbose_mode;

extern int errno;
uint32 relpathlen;
extern ptr < rabin_priv > sfsrokey;

qhash<chordID, bool, hashID> dup_cache;

/* Statistics */
u_int32_t reginode_cnt = 0;
u_int32_t lnkinode_cnt = 0;
u_int32_t filedatablk_cnt = 0;
u_int32_t sindir_cnt = 0;
u_int32_t dindir_cnt = 0;
u_int32_t tindir_cnt = 0;
u_int32_t directory_cnt = 0;
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

  create_sfsrofh (fh, callbuf, calllen);

  // Store the inode of this path in the database
  sfsrodb_put (fh->base (), fh->size (), callbuf, calllen);
  fh_cnt++;
  if (inode->type == SFSROLNK)
    lnkinode_cnt++;
  else
    reginode_cnt++;

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

  create_sfsrofh (fh, callbuf, calllen);

  sfsrodb_put (fh->base (), fh->size (), callbuf, calllen);

  filedatablk_cnt++;
  fh_cnt++;
      
  xfree (callbuf);
  return true;
}

inline bool
process_sindirect (int &fd, bool &wrote_stuff, sfsro_inode *inode,
		   char *block, sfs_hash &block_fh, sfs_hash *fh) {

  size_t size = 0;

  uint32 blocknum = 0;
  sfsro_data sindir (SFSRO_INDIR);
  sindir.indir->handles.setsize (nfh);
  
  while (blocknum < nfh) {
    
    size = read (fd, block, blocksize);

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
    
    create_sfsrofh (fh, callbuf, calllen);

    sfsrodb_put (fh->base(), fh->size(),
		 callbuf, calllen);
    
    sindir_cnt++;
    fh_cnt++;
    
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
  dindir.indir->handles.setsize (nfh);
  // XXX we could be smarter here by only allocating
  // the number of file handles we actually need in the dindirect.

  while (!done && blocknum < nfh) {

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
    
    create_sfsrofh (fh, callbuf, calllen);

    sfsrodb_put (fh->base(), fh->size(),
		 callbuf, calllen);
  
    dindir_cnt++;
    fh_cnt++;
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
  tindir.indir->handles.setsize (nfh);
  // XXX we could be smarter here by only allocating
  // the number of file handles we actually need in the tindirect.
  
  while (!done && blocknum < nfh) {
    
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
    
    create_sfsrofh (fh, callbuf, calllen);

    sfsrodb_put (fh->base(),
		 fh->size(),
		 callbuf, calllen);
    tindir_cnt++;
    fh_cnt++;
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
  char *block = New char[blocksize];
  while (blocknum < SFSRO_NDIR) {      
    
    size = read (fd, block, blocksize);
    
    if (size <= 0)
      break;

    inode->reg->size += size;

   
    /* Check for identical blocks */
    if (store_file_block (&block_fh, block, size)) {
      inode->reg->used += size;
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

  delete block;
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

  create_sfsrofh (fh, callbuf, calllen);

  sfsrodb_put (fh->base (), fh->size (), callbuf, calllen);

  directory_cnt++;
  fh_cnt++;
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

  qsort (file_list.base (), file_list.size (),
	 sizeof (char *), compare_name);

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

   Path is like '/','/a','/a/b' (ie only the root ends in slash)

   Return: The hash and IV in the fh.

   Effects: Recursively hash everything beneath a directory
   It computes the cryptographic file handle for the
   given path, inserts the mapping from the fh to its data

 */
int
recurse_path (const str path, sfs_hash * fh)
{
  struct stat st;

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

    sfsrodb_setinode (&st, &inode);
    inode.lnk->dest = nfspath3 (buf, nchars);

    delete[] buf;

  } else if (S_ISREG (st.st_mode)) {
    sfsrodb_setinode (&st, &inode);
    store_file (&inode, path);
  }
  else if (S_ISDIR (st.st_mode)) {
    sfsrodb_setinode (&st, &inode);
    vec < char *>file_list;
    sort_dir (path, file_list);
    sfsro_data directory (SFSRO_DIRBLK);

    // XXXXX need to take of the prefix of path
    if (path.len() ==  relpathlen)
      directory.dir->path = str ("/");
    else
      directory.dir->path = substr (path, relpathlen);

    directory.dir->eof = true;
    rpc_ptr < sfsro_dirent > *direntp = &directory.dir->entries;

    for (unsigned int i = 0; i < file_list.size (); i++) {
      (*direntp).alloc ();
      (*direntp)->name = file_list[i];

      if (((*direntp)->name.cmp (".") == 0) ||
	  ((*direntp)->name.cmp ("..") == 0)) {
	continue;
      }
      else {
	recurse_path (path << "/" << (*direntp)->name,
		      &(*direntp)->fh);

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
sfsrodb_main (const str root, const str keyfile)
{

  int fd = unixsocket_connect(LSD_SOCKET);
  if (fd < 0)
    fatal << "error connecting to " << LSD_SOCKET << "\n";
  
  cclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  
  ptr < rabin_priv > sk;
  if (!keyfile) {
    warn << "cannot locate default file sfs_host_key\n";
    fatal ("errors!\n");
  }
  else {
    str key = file2wstr (keyfile);
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

  // store file system in db
  sfs_hash root_fh;

  relpathlen = root.len ();
  recurse_path (root, &root_fh);

  sfsro_data dat (CFS_FSINFO);

  dat.fsinfo->pubkey = sk->n;
  dat.fsinfo->info.rootfh = fh2mpz(root_fh.base (), root_fh.size ());
  time_t start, end;
  dat.fsinfo->info.start = start = time (NULL);
  dat.fsinfo->info.duration = sfsro_duration;
  dat.fsinfo->info.blocksize = blocksize;
  end = start + sfsro_duration;

  // XX Should make sure timezone is correct
  str stime (ctime (&start));
  str etime (ctime (&end));
  warn << "Database good from: \n " << stime
       << "until:\n " << etime;

  dat.fsinfo->sig = sk->sign (xdr2str (dat));
  dat.fsinfo->pubkey = sk->n;
  
  char rootfh_hash[20];
  str raw = sk->n.getraw ();
  sha1_hash (rootfh_hash, raw.cstr (), raw.len ());
  str rootfh_name = armor64A(rootfh_hash, 20);
  warn << "exporting file system under " << rootfh_name << "\n";

  xdrsuio x (XDR_ENCODE);
  if (xdr_sfsro_data (x.xdrp (), &dat)) {
    void *v = suio_flatten (x.uio ());
    int l =  x.uio ()->resid ();
    sfsrodb_put (rootfh_hash, 20, v, l);
  }

  if (verbose_mode) {
    warn << "identical blocks:   " << identical_block << "\n";
    warn << "identical sindirs:  " << identical_sindir << "\n";
    warn << "identical dindirs:  " << identical_dindir << "\n";
    warn << "identical tindirs:  " << identical_tindir << "\n";
    warn << "identical dirs:     " << identical_dir << "\n";
    warn << "identical inodes:   " << identical_inode << "\n";
    warn << "identical symlinks: " << identical_sym << "\n";

    warn << "Database contents:\n";
    warn << "Regular inodes:      " << reginode_cnt << "\n";
    warn << "Symlink inodes:      " << lnkinode_cnt << "\n";
    warn << "Directory blocks     " << directory_cnt << "\n";
    warn << "File data blocks:    " << filedatablk_cnt << "\n";
    warn << "Single indir blocks: " << sindir_cnt << "\n";
    warn << "Double indir blocks: " << dindir_cnt << "\n";
    warn << "Triple indir blocks: " << tindir_cnt << "\n";

    warn << "identical fh's overall : " << identical_fh << "\n";
    warn << "unique fh's overall    : " << fh_cnt << "\n\n\n";
  }
  
  while (out) acheck ();
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
  warnx << "Optional directives:\n";
  warnx << "-v                    : Verbose debugging output\n";
  warnx << "-b <blocksize>        : Page size of underlying database\n";

  exit (1);
}


int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  char *exp_dir = NULL;
  char *sk_file = NULL;

  verbose_mode = false;
  initialize = false;
  blocksize = 8192;
  
  int ch;
  while ((ch = getopt (argc, argv, "b:d:s:v")) != -1)
    switch (ch) {
    case 'b':
      if (!convertint (optarg, &blocksize)
	  || blocksize < 512 || blocksize > 0x400000)
	usage ();
      break;
    case 'd':
      exp_dir = optarg;
      break;
    case 's':
      sk_file = optarg;
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
  nfh = (blocksize - 100) / (20*2);

  if ( (argc > 0) || !exp_dir || !sk_file )
    usage ();

  if (verbose_mode) {
    warnx << "export directory : " << exp_dir << "\n";
    warnx << "SK keyfile       : " << sk_file << "\n";
  }

  return (sfsrodb_main (exp_dir, sk_file));
}
