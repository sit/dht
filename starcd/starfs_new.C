/*
 *
 * Copyright (C) 2001  John Bicket (jbicket@mit.edu),
 *                     Sanjit Biswas (biswas@mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include "starcd.h"
#include "rxx.h"
#include "arpc.h"
#include "xdr_suio.h"
#include "afsnode.h"
#include "quorum.h"
#include "parseopt.h"
#include <stdlib.h>
#include "dhash.h"

#define LSD_SOCKET "/tmp/chord-sock"
int count = 0;

dhashclient dhash (LSD_SOCKET);


void 
check_data_cb (dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK || !i) {
    fatal << "failed on insert\n";
  }

  count++;
  if (count == 3) {
    exit(0);
  }
}


chordID put_immutable_data(const char *buf, size_t buflen) {
  chordID key = compute_hash(buf, buflen);
  dhash.insert (key, buf, buflen, wrap(&check_data_cb));
  return key;
}

void put_mutable_data(chordID ID, int num_replicas, const char *buf, size_t buflen) {
  quorum_write (dhash, ID, num_replicas, 
	       NULL, 0,
	       buf, buflen, 
	       wrap(&check_data_cb));
}



void 
usage ()
{
 warnx << "usage: " << progname
       << "-r <num_replicas>\n";
   exit (1);
}


chordID random_key() {
  return random_bigint(160);
}

void store_metaroot(chordID root, int num_replicas) {
  starfs_data metaroot (STARFS_FSINFO);
  chordID metaroot_key;
  /* set up the metaroot info */
  metaroot.fsinfo->rootfh = root;
  metaroot.fsinfo->blocksize = 8192;
  metaroot.fsinfo->replicas = num_replicas;


  xdrsuio x (XDR_ENCODE);
  if (xdr_starfs_data (x.xdrp (), &metaroot)) {
    char *v = suio_flatten (x.uio ());
    int l =  x.uio ()->resid ();
    metaroot_key = put_immutable_data(v, l);
    xfree (v);
  }

}


/* Given: A fully specified inode
   Effects: Stores the inode in the database with the fh as the key
   Return: A file handle in fh.  fh must already be allocated.
   This function will set the IV appropriately with prandom bits.
 */
void
store_inode (starfs_inode *inode, chordID key, int num_replicas)
{
  starfs_data dat (STARFS_INODE);
  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);

  *dat.inode = *inode;
  if (xdr_starfs_data (x.xdrp (), &dat)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  // Store the inode of this path in the database
  put_mutable_data (key, num_replicas, callbuf, calllen);
  xfree (callbuf);
}



/* Given: a fully specified directory, inode filled in by setinode,
   allocated fh

   Return: file handle, store directory contents, final inode values

   Effects: After filling in a directory structure, fill in an inode
   structure, store the directory in the database, and compute file
   handle.  
 */
void
store_directory (starfs_inode *inode,
		 starfs_data *directory)
{
  // XXX???  sfsro_data dat (SFSRO_DIRBLK);
  size_t calllen = 0;
  chordID key;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);

  if (xdr_starfs_data (x.xdrp (), directory)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  key = put_immutable_data (callbuf, calllen);

  xfree (callbuf);

  // This is bogus, we're just using one data pointer regardless
  // of the size of the directory.  In a correct implementation,
  // we should only store 8KB per data block
  inode->reg->size = inode->reg->used = calllen;
  inode->reg->direct.setsize (1);
  inode->reg->direct[0] = key;

}


/* returns the address of the meta root */
void create_root(int num_replicas) {
  starfs_inode inode;
  chordID root_key;

  root_key = random_key();
  /* set the inode up */
  inode.set_type (STARFSDIR);
  //rpc_clear(inode.reg); /* ??? jbicket */
  inode.reg->path = "/";
  inode.reg->nlink = 0;
  inode.reg->size = 0;
  inode.reg->used = 0;

  inode.reg->mtime.seconds = 0;
  inode.reg->mtime.nseconds = 0;
  inode.reg->ctime.seconds = 0;
  inode.reg->ctime.nseconds = 0;


  inode.reg->direct.setsize (0);

  /* set the directory up ? */
  starfs_data directory (STARFS_DIRBLK);
  directory.dir->eof = true;
  
  store_directory(&inode, &directory);
  store_inode(&inode, root_key, num_replicas);
  store_metaroot(root_key, num_replicas);    
}



int main(int argc, char **argv) {

  u_int32_t num_replicas = 1;
  static rxx path_regexp ("([0-9a-zA-Z+-]*)$");

  setprogname (argv[0]);
  
  int ch;
  while ((ch = getopt (argc, argv, "b:d:s:g:vwx")) != -1)
    switch (ch) {
    case 'r':
      if (!convertint (optarg, &num_replicas)) {
	usage ();
      }
      break;
    case '?':
    default:
      usage ();
      return 0;
    }
  random_init ();
  
   
  create_root(num_replicas);
  
}

