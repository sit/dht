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

// 
// fileids == lower_64_bits (sha1(path))
// sfs_hash == chordID == nfs_fh3
//

#include "starcd.h"
#include "rxx.h"
#include "arpc.h"
#include "xdr_suio.h"
#include "afsnode.h"
#include "quorum.h"
#include<stdlib.h>

#define LSD_SOCKET "/tmp/chord-sock"
#define CMTU 1024
#define PREFETCH_BLOCKS 8
#define MAX(a,b) (((a)>(b))?(a):(b))
#define num_replicas 10


chord_server::chord_server (u_int cache_maxsize)
  : dhash (LSD_SOCKET)
{
  random_init();
  count = random_getword();
}

void
chord_server::setrootfh (str root, cbfh_t rfh_cb)
{
  static rxx path_regexp ("chord:([0-9a-zA-Z+-]*)$");

  if (!path_regexp.search (root)) {
    warn << "Malformated root filesystem name: " << root << "\n";
    (*rfh_cb) (NULL);
  }
  else {
    str rootfhstr = path_regexp[1];
    str dearm = dearmor64A (rootfhstr);
    chordID ID;
    mpz_set_rawmag_be (&ID, dearm.cstr (), dearm.len ());
    //fetch the root file handle too..
  }
}



chordID 
chord_server::nfsfh_to_chordid (nfs_fh3 *nfh) 
{
  chordID n;
  mpz_set_rawmag_be (&n, nfh->data.base (), nfh->data.size ());
  return n;
}

void
chord_server::chordid_to_nfsfh (chordID *n, nfs_fh3 *nfh)
{
  str raw = n->getraw ();
  nfh->data.setsize (raw.len ());
  memcpy (nfh->data.base (), raw.cstr (), raw.len ());
}

void
chord_server::dispatch (ref<nfsserv> ns, nfscall *sbp)
{

  switch(sbp->proc()) {
  case NFSPROC3_READLINK:
    {

    }
    break;
  case NFSPROC3_GETATTR: 
    {

    }
    break;
  case NFSPROC3_SETATTR:
    {

    } 
    break;
  case NFSPROC3_FSSTAT:
    {
    }
    break;
  case NFSPROC3_FSINFO:
    {

    }
    break;
  case NFSPROC3_ACCESS:
    {

    }
    break;
  case NFSPROC3_LOOKUP: 
    {
    }
    break;
  case NFSPROC3_CREATE:
    {

    }
    break;
  case NFSPROC3_MKDIR:
    {

    }
    break;
  case NFSPROC3_REMOVE:
    {

    }
    break;  
  case NFSPROC3_READDIR:
    {

    }
    break;
  case NFSPROC3_READDIRPLUS:
    {
    }
    break;
  case NFSPROC3_READ:
    {

    }
    break;
  case NFSPROC3_NULL:
    {
      sbp->reply (NULL);
      return;
    }
    break;
  case NFSPROC_CLOSE:
    {

    }
    break;
  case NFSPROC3_WRITE:
    {

    }
    break;
  case NFSPROC3_RENAME:
    {

    }
    break;
  default:
    {
      sbp->error (NFS3ERR_NOTSUPP);
      return;
    }
    break;
  }
}

        

void
chord_server::fetch_data_cb (chordID ID, cbdata_t cb, ptr<dhash_block> blk)
{
  ptr<starfs_data> data = NULL;

  if (blk) {
    data = New refcounted<starfs_data>;
    xdrmem x (blk->data, blk->len, XDR_DECODE);
    if (xdr_starfs_data (x.xdrp (), data)) {

    } else {
      warn << "Couldn't unmarshall data\n";
    }
  }

  
  (cb) (data);
}

void chord_server::fetch_immutable_data(chordID ID, cbdata_t cb) {
  dhash.retrieve (ID, wrap (this, &chord_server::fetch_data_cb, ID, cb));
}
void chord_server::fetch_mutable_data(chordID ID, cbdata_t cb) {
  quorum_read (dhash, ID, num_replicas, wrap (this, &chord_server::fetch_data_cb, ID, cb));
}

