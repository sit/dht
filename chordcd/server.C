/*
 *
 * Copyright (C) 2001  Josh Cates (cates@mit.edu),
 *                     Frank Dabek (fdabek@lcs.mit.edu), 
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

// KNOWN ISSUES:
//   * READDIR
//     - client always re-requests entries in a directory, why?
//   * READDIRP
//     - completely broken/not really implemented
//   * READ
//     - does not deal with request which span blocks
//     - pre-fetching not implemented
//   * LOOKUP
//     - linear scan algo could be improved, to return NOENT
//       when it is sure that the entry does not exists
//     - or binary lookup could be implemented
//   * GENERAL
//     - possibly, embedded inodes to improve LOOKUP performance
//     - can make lots of optimizations to filesystem structure which are easy when
//       the FS is read-only, but hard in a read-write filesystem, namely embedded inodes
//     - copy the dispatch routine from sfsrocd
//   * XXX
//     - search for this and find more things to fix



// 
// fileids == lower_64_bits (sha1(path))
// sfs_hash == chordID == nfs_fh3
//

#include <dhash_common.h>
#include "chordcd.h"
#include "rxx.h"
#include "arpc.h"
#include "xdr_suio.h"
#include "afsnode.h"

#define LSD_SOCKET "/tmp/chord-sock"
#define CMTU 1024
#define PREFETCH_BLOCKS 8

static void splitpath (vec<str> &out, str in);
static fattr3 ro2nfsattr (sfsro_inode *si);


// XXX keep in sync with same function in sfsrodb/sfsrodb.C
static uint64
path2fileid(const str path)
{
  char buf[sha1::hashsize];
  bzero(buf, sha1::hashsize);
  sha1_hash (buf, path.cstr (), path.len ());
  return gethyper (buf); // XXX not using all 160 bits of the hash
}


chord_server::chord_server (u_int cache_maxsize)
  : dh (LSD_SOCKET), data_cache (cache_maxsize)
{

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
    fetch_data (false, ID, DHASH_KEYHASH, 
		wrap (this, &chord_server::getroot_fh, rfh_cb));
  }
}


void
chord_server::getroot_fh (cbfh_t rfh_cb, ptr<sfsro_data> d)
{
  if (!d) {
    warn << "root block was null\n";
    (*rfh_cb) (NULL);
  } else if (d->type  != CFS_FSINFO) {
    warn << "root block had wrong type\n";
    (*rfh_cb) (NULL);
  } else {
    warn << "Mounted filesystem.\n";
    warn << "  start     : " << ctime(&(time_t)d->fsinfo->start);
    warn << "  duration  : " << d->fsinfo->duration  << " seconds \n";
    warn << "  blocksize : " << d->fsinfo->blocksize << "\n";
    this->rootdirID = d->fsinfo->rootfh;
    this->fsinfo = *d->fsinfo;
    fetch_data (false, rootdirID, wrap (this, &chord_server::getrootdir_cb, rfh_cb));
  }
}

 
void
chord_server::getrootdir_cb (cbfh_t rfh_cb, ptr<sfsro_data> data)
{
  if (data && data->type == SFSRO_INODE && data->inode->type == SFSRODIR) {
    this->rootdir = data;

    // XXX verify file handle
    nfs_fh3 *fh = New nfs_fh3;
    chordid_to_nfsfh (&rootdirID, fh);
    (*rfh_cb) (fh);
  } else {
    warn << "root directory invalid\n";
    (*rfh_cb) (NULL);
  }
}


void
chord_server::dispatch (ref<nfsserv> ns, nfscall *sbp)
{

  switch(sbp->proc()) {
  case NFSPROC3_READLINK:
    {
    nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
    chordID fh = nfsfh_to_chordid (nfh);
    fetch_data (false, fh, wrap (this, &chord_server::readlink_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_GETATTR: 
    {
      nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::getattr_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_FSSTAT:
    {
      fsstat3res res (NFS3_OK);
      rpc_clear (res);
      sbp->reply (&res);
    }
    break;
  case NFSPROC3_FSINFO:
    {
      int blocksize = fsinfo.blocksize;
      fsinfo3res res (NFS3_OK);
      res.resok->rtmax = blocksize;
      res.resok->rtpref = blocksize;
      res.resok->rtmult = 512;
      res.resok->wtmax = blocksize;
      res.resok->wtpref = blocksize;
      res.resok->wtmult = 8192;
      res.resok->dtpref = blocksize;
      res.resok->maxfilesize = INT64 (0x7fffffffffffffff);
      res.resok->time_delta.seconds = 0;
      res.resok->time_delta.nseconds = 1;
      res.resok->properties = (FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS);
      sbp->reply (&res);
    }
    break;
  case NFSPROC3_ACCESS:
    {
      access3args *aa = sbp->template getarg<access3args> ();
      nfs_fh3 *nfh = &(aa->object);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::access_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_LOOKUP: 
    {
      diropargs3 *dirop = sbp->template getarg<diropargs3> ();

      nfs_fh3 *nfh = &(dirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::lookup_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READDIR:
    {
      readdir3args *readdirop = sbp->template getarg<readdir3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::readdir_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READDIRPLUS:
    {
      readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::readdirp_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READ:
    {
      read3args *ra = sbp->template getarg<read3args> ();
      nfs_fh3 *nfh = &(ra->file);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (false, fh, wrap (this, &chord_server::read_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_NULL:
    {
      sbp->reply (NULL);
      return;
    }
    break;
  case NFSPROC_CLOSE:
    break;
  default:
    {
      sbp->error (NFS3ERR_ROFS);
      return;
    }
    break;
  }
}


// ---------------- operations ---------------
void
chord_server::readlink_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  if (!data)
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);    
  else if (data->inode->type != SFSROLNK)
    sbp->error (NFS3ERR_IO);
  else if (data->inode->lnk->dest.len () <= 0)
    sbp->error (NFS3ERR_INVAL);
  else {
    readlink3res nfsres (NFS3_OK);
    nfsres.resok->data = data->inode->lnk->dest;
    sbp->reply(&nfsres);
  }
}



void
chord_server::getattr_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data) 
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);    
  else {
    getattr3res nfsres (NFS3_OK);
    *nfsres.attributes = ro2nfsattr (data->inode);
    sbp->reply (&nfsres);
  }
}



void
chord_server::access_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data) 
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);    
  else {
    access3args *aa = sbp->template getarg<access3args> ();
    uint32 access_req = aa->access;
    access3res nfsres (NFS3_OK);
    
    nfsres.resok->access = access_check (data->inode, access_req);
    
    sbp->reply (&nfsres);
  }
}


uint32
chord_server::access_check(sfsro_inode *ip, uint32 access_req)
{
  uint32 r = 0;

  switch (ip->type) { 
  case SFSRODIR:
    r = ACCESS3_READ | ACCESS3_LOOKUP;
    break;
  case SFSRODIR_OPAQ:
    r = ACCESS3_LOOKUP;
    break;
  case SFSROREG_EXEC:
    r = ACCESS3_READ | ACCESS3_EXECUTE;
    break;
  case SFSROLNK:
  case SFSROREG:
    r = ACCESS3_READ;
    break;
  }

  return (access_req & r);
}


void
chord_server::lookup_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ)
    sbp->error (NFS3ERR_NOTDIR);
  else {
    diropargs3 *dirop = sbp->template getarg<diropargs3> ();
    lookup (data, ID, dirop->name,
	    wrap (this, &chord_server::lookup_lookup_cb, sbp, data, ID));
  }
}


void
chord_server::lookup_lookup_cb (nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID, 
				ptr<sfsro_data> data, chordID dataID, nfsstat3 status)
{
  if (status == NFS3_OK) {
    lookup3res *nfsres = sbp->template getres<lookup3res> ();
    nfsres->set_status (NFS3_OK);
    nfsres->resok->dir_attributes.set_present (true);

    *nfsres->resok->dir_attributes.attributes = ro2nfsattr (dirdata->inode);

    nfs_fh3 nfh;
    chordid_to_nfsfh (&dataID, &nfh);
    
    nfsres->resok->object = nfh;
    nfsres->resok->obj_attributes.set_present (true);

    *nfsres->resok->obj_attributes.attributes =  ro2nfsattr (data->inode);

    sbp->reply (nfsres);
  } else {
    sbp->error (status);
  }
}



void
chord_server::readdir_inode_cb (nfscall *sbp, chordID dataID, ptr<sfsro_data> data)
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ)
    sbp->error (NFS3ERR_NOTDIR);
  else {
    readdir3args *nfsargs = sbp->template getarg<readdir3args> ();
    uint32 blockno = nfsargs->cookie >> 32;
    read_file_data (false, blockno, data->inode->reg, 
		    wrap (this, &chord_server::readdir_fetch_dirdata_cb,
			  sbp, data, dataID));
  }
}


void
chord_server::readdir_fetch_dirdata_cb (nfscall *sbp,
					ptr<sfsro_data> dirInodeData,
					chordID dirInodeID,
					ptr<sfsro_data> dirblk)
{
  if (dirblk == NULL) {
      sbp->error (NFS3ERR_IO);
  } else if (dirblk->type != SFSRO_DIRBLK) {
      sbp->error (NFS3ERR_IO);
  } else {
    readdir3args *nfsargs = sbp->template getarg<readdir3args> ();
    uint32 blockno = nfsargs->cookie >> 32;
    uint32 entryno = (uint32)nfsargs->cookie;
    sfsro_directory *dir = dirblk->dir;

    readdir3res nfsres (NFS3_OK);
    nfsres.resok->dir_attributes.set_present (true);
    *nfsres.resok->dir_attributes.attributes = ro2nfsattr(dirInodeData->inode);
    nfsres.resok->reply.eof = false;
    rpc_ptr<entry3> *direntp = &nfsres.resok->reply.entries;
    sfsro_dirent *roe = dir->entries;

    int space = nfsargs->count - 184; // reserve hdr and trailing NULL dirent

    //warn << "------ cookie " << nfsargs->cookie << " ---------\n";
    for (uint i = 1; space > 0 && roe ; roe = roe->nextentry, i++) {
      if (i <= entryno)
	continue;
      //warn << "i " << i << ", entryno " << entryno  << ", space " << space;
      //warn << ", roe[" << roe->name << ", fileid " << roe->fileid << "]\n";
      space -= ((roe->name.len() + 3) & ~3) + 24; // marshaled entry size
      if (space >= 0) {
	(*direntp).alloc ();
	(*direntp)->name = roe->name;
	(*direntp)->fileid = roe->fileid;
	(*direntp)->cookie = (uint64 (blockno)) << 32 | i;
	if (!roe)
	  (*direntp)->cookie = (uint64 (blockno + 1)) << 32;
	direntp = &(*direntp)->nextentry;
      }
    }
    
    nfsres.resok->reply.eof = (!roe && dir->eof);
    sbp->reply (&nfsres);
  }
}



void
chord_server::readdirp_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ)
    sbp->error (NFS3ERR_NOTDIR);
  else {
    fetch_data (false, sfshash_to_chordid(&data->inode->reg->direct[0]),
		wrap (this, &chord_server::readdirp_fetch_dir_data, 
		      sbp, data, ID));
  }
}


void
chord_server::readdirp_fetch_dir_data (nfscall *sbp, ptr<sfsro_data> dirdata,
				       chordID dirID,
				       ptr<sfsro_data> data) 
{
  sfsro_directory *dir = data->dir;
  readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();
  
  readdirplus3res *nfsres = sbp->template getres<readdirplus3res> ();
  rpc_clear (*nfsres);
  nfsres->set_status (NFS3_OK);
  nfsres->resok->dir_attributes.set_present (true);
  *nfsres->resok->dir_attributes.attributes = ro2nfsattr (dirdata->inode);  
  size_t off = readdirop->cookie;
  
  sfsro_dirent *roe = dir->entries;
  uint32 i = 0;
  while (roe) {
    roe = roe->nextentry;
    i++;
  }
  uint32 n = MIN(i, readdirop->maxcount);
  roe = dir->entries;
  rpc_ptr<entryplus3> *last = &(nfsres->resok->reply.entries);

  for (uint32 j = 0; j < n; j++) {
    (*last).alloc ();
    assert (0);
    //fh2fileid (&roe->fh, &(*last)->fileid); // XXX broken
    (*last)->name = roe->name;
    
    // XXX Make LINUX happy
    off +=  sizeof ((*last)->fileid) + roe->name.len();
    if (j + 1 >= n) (*last)->cookie = INT64 (0x400ffffffff);
    else (*last)->cookie = off;

    (*last)->name_attributes.set_present(0);
    (*last)->name_handle.set_present(0);
    last = &(*last)->nextentry;
    roe = roe->nextentry;
  }
  nfsres->resok->reply.eof = (i < n) ? 0 : 1;

  sbp->reply (nfsres);
}


void
chord_server::read_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  read3args *ra = sbp->template getarg<read3args> ();

  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSROREG && data->inode->type != SFSROREG_EXEC) {
    sbp->error (NFS3ERR_IO);
  }
  else if (ra->offset >= data->inode->reg->size) {
    read3res nfsres(NFS3_OK);
    
    nfsres.resok->count = 0;
    nfsres.resok->eof = 1;
    nfsres.resok->file_attributes.set_present(1);
    *nfsres.resok->file_attributes.attributes = ro2nfsattr(data->inode);
    sbp->reply(&nfsres);
  }
  else {
    //XXX if a read straddles two blocks we will do a short read 
    ///   i.e. only read the first block
    size_t block = ra->offset / fsinfo.blocksize;
    read_file_data (false, block, data->inode->reg, 
		    wrap (this, &chord_server::read_data_cb,
			  sbp, data, ID));

    size_t ftch_lim = block + PREFETCH_BLOCKS;
    size_t file_bytes = roundup (data->inode->reg->size, fsinfo.blocksize);
    size_t file_blks = file_bytes / fsinfo.blocksize;

    for (size_t b = block + 1; b <= ftch_lim && b < file_blks; b++)
      read_file_data (true, b, data->inode->reg, wrap (this, &chord_server::ignore_data_cb));
  }
}

void
chord_server::ignore_data_cb (ptr<sfsro_data> data)
{
}

void
chord_server::read_data_cb (nfscall *sbp, ptr<sfsro_data> inode, chordID inodeID,
			    ptr<sfsro_data> data)
{
  if (!data)
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_FILEBLK)
    sbp->error (NFS3ERR_STALE);		
  else {
    char *buf = data->data->base();
    size_t size = data->data->size();

    read3args *ra = sbp->template getarg<read3args> ();
    read3res nfsres (NFS3_OK);
    
    fattr3 fa = ro2nfsattr(inode->inode);
    unsigned int blocksize = (unsigned int)fsinfo.blocksize;
    
    size_t start = ra->offset % blocksize;
    size_t n = MIN (MIN (ra->count, size), size - start);
    
    nfsres.resok->count = n;
    nfsres.resok->eof = (fa.size >= ra->offset + n) ? 1 : 0;

    // XXX avoid a data copy by marshalling directly into the xdr buffer
    nfsres.resok->data.setsize(n);
    memcpy (nfsres.resok->data.base(), buf + start, 
	    nfsres.resok->data.size ()); 
    nfsres.resok->file_attributes.set_present(1);
    *nfsres.resok->file_attributes.attributes = fa;
    sbp->reply (&nfsres);
  }
}






// ------------------------ util -------------------


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

chordID
chord_server::sfshash_to_chordid (sfs_hash *fh) 
{
  bigint n;
  mpz_set_rawmag_be (&n, fh->base (), fh->size ());
  return n;
}

static fattr3 
ro2nfsattr (sfsro_inode *si)
{
  fattr3 ni;
  // XXX this is duplicated code.  It 
  // should be fixed in the spec file.

  if (si->type == SFSROLNK) {
    ni.nlink = si->lnk->nlink;
    ni.size = si->lnk->dest.len ();
    ni.used = 0;
    ni.mtime = si->lnk->mtime;
    ni.ctime = si->lnk->ctime;
    ni.atime = si->lnk->mtime;
    ni.fileid = path2fileid(si->lnk->path);
  } else {
    ni.nlink = si->reg->nlink;
    ni.size = si->reg->size;
    ni.used = si->reg->used;
    ni.mtime = si->reg->mtime;
    ni.ctime = si->reg->ctime;
    ni.atime = si->reg->mtime;
    ni.fileid = path2fileid(si->reg->path);
  }

  /* Below are synthesized attributes */
  ni.mode = 0444;
  
  switch (si->type) {
  case SFSROREG_EXEC:
    ni.mode = 0555;
  case SFSROREG:
    ni.type = NF3REG;
    break;
  case SFSRODIR:
    ni.mode = 0555;
    ni.type = NF3DIR;
    break;
  case SFSRODIR_OPAQ:
    ni.mode = 0111;
    ni.type = NF3DIR;
    break;
  case SFSROLNK:
    ni.type = NF3LNK;
    break;
  default:
    fatal ("server::ro2nfsattr: Unknown si->type %X\n",
	   si->type);
    break;
  }
    
  ni.uid = sfs_uid;
  ni.gid = sfs_gid;
  ni.rdev.minor = 0;
  ni.rdev.major = 0;
  ni.fsid = 0;

  return ni;
}


// ------------------------ lookup ------------------

struct lookup_state {
  ptr<sfsro_data> dirdata;
  sfsro_inode_reg *dirinode;
  str component;
  cblookup_t cb;

  ssize_t curblock; // linear scan state variables
  ssize_t maxblock;

  lookup_state (ptr<sfsro_data> dirdata, str component, cblookup_t cb,
		size_t blocksize) :
    dirdata (dirdata), component (component), cb (cb)
  {
    assert (dirdata->type == SFSRO_INODE); 
    assert (dirdata->inode->type == SFSRODIR ||
	    dirdata->inode->type == SFSRODIR_OPAQ);
    dirinode = dirdata->inode->reg;

    curblock = 0;
    maxblock = -1; 
    if (dirinode->direct.size() > 0)
      maxblock = (dirinode->direct.size() - 1)  / blocksize;
  }
};


void
chord_server::lookup(ptr<sfsro_data> dirdata, chordID dirID, str component, cblookup_t cb)
{
  if (!dirdata) {
    (*cb) (NULL, 0, NFS3ERR_STALE);
  } else if (dirdata->type != SFSRO_INODE) {
    (*cb) (NULL, 0, NFS3ERR_IO);
  } else if (dirdata->inode->type != SFSRODIR && 
	     dirdata->inode->type != SFSRODIR_OPAQ) {  
    (*cb) (NULL, 0, NFS3ERR_IO);    
  } else if (component == ".") {
    (*cb) (dirdata, dirID, NFS3_OK);
  } else if (component == "..") {
    namei (dirdata->inode->reg->path, cb);
  } else {
    ref<lookup_state> st =
      New refcounted<lookup_state> (dirdata, component, cb, fsinfo.blocksize);
    lookup_scandir_nextblock(st);
  }
}


void
chord_server::lookup_scandir_nextblock(ref<lookup_state> st)
{
  if (st->curblock > st->maxblock) {
    (*st->cb) (NULL, 0, NFS3ERR_NOENT);
  } else {
    size_t curblock = st->curblock;
    st->curblock++;
    read_file_data (false, curblock, st->dirinode, 
    		    wrap (this, &chord_server::lookup_scandir_nextblock_cb, st));
  }
}


void
chord_server::lookup_scandir_nextblock_cb(ref<lookup_state> st, ptr<sfsro_data> dat)
{
  if (dat == NULL || dat->type != SFSRO_DIRBLK) 
    (*st->cb) (NULL, 0, NFS3ERR_STALE);
  else {
    sfsro_dirent *dirent = dirent_lookup (st->component, dat->dir);
    if (dirent) {
      chordID componentID = sfshash_to_chordid (&dirent->fh);
      fetch_data (false, componentID,
		  wrap (this, &chord_server::lookup_component_inode_cb, st, componentID));
    } else {
      lookup_scandir_nextblock(st);
    }
  }
}



void
chord_server::lookup_component_inode_cb (ref<lookup_state> st, chordID ID, ptr<sfsro_data> data)
{
  if (!data) 
    (*st->cb) (NULL, 0, NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    (*st->cb) (NULL, 0, NFS3ERR_IO);
  else {
    (*st->cb) (data, ID, NFS3_OK);
  }
}


sfsro_dirent *
chord_server::dirent_lookup (str name, sfsro_directory *dir)
{
  assert (name != ".");
  assert (name != "..");

  sfsro_dirent *e = NULL, *e_prev = NULL;

  for (e = e_prev = dir->entries; e; e = e->nextentry) {

     if (name == e->name)
	 return e;

     if ((e_prev->name < name) && (name < e->name))
	 return NULL;
     e_prev = e;
  }
  return NULL;
}




// ---------------------------- namei -----------------------

struct namei_state {
  str path;
  vec<str> components;
  cbnamei_t cb;

  namei_state (str path, cbnamei_t cb) : path (path), cb (cb) {
    splitpath (components, path);
  };
};

// NAMEI translate a path to an inode
void
chord_server::namei (str path, cbnamei_t cb)
{
  ref<namei_state> st = New refcounted<namei_state>(path, cb);
  namei_iter (st, this->rootdir, this->rootdirID);
}


void
chord_server::namei_iter (ref<namei_state> st, ptr<sfsro_data> inode, chordID inodeID)
{
  if (!st->components.size ()) {
    (*st->cb) (inode, inodeID, NFS3_OK);
  } else if (inode->inode->type != SFSRODIR &&
	     inode->inode->type != SFSRODIR_OPAQ) {  
    (*st->cb) (NULL, 0, NFS3ERR_IO);
  } else {
    str component = st->components.pop_front ();
    lookup (inode, inodeID, component, wrap (this, &chord_server::namei_iter_cb, st));
  }
}


void
chord_server::namei_iter_cb (ref<namei_state> st,
			     ptr<sfsro_data> data, chordID dataID, nfsstat3 status)
{
  if (status != NFS3_OK) {
    (*st->cb) (NULL, 0, NFS3ERR_STALE);
  } else {
    namei_iter (st, data, dataID);
  }
}


static void
splitpath (vec<str> &out, str in)
{
  const char *p = in.cstr ();
  const char *e = p + in.len ();
  const char *n;

  for (;;) {
    while (*p == '/')
      p++;
    for (n = p; n < e && *n != '/'; n++)
      ;
    if (n == p)
      return;
    out.push_back (str (p, n - p));
    p = n;
  }
    
}


// -------------------------- bmap --------------------------

void
chord_server::bmap (bool pfonly, size_t block, sfsro_inode_reg *inode, cbbmap_t cb)
{
  chordID ID;

  // XXX this is the same calculation that is at the end of sfsrodb/sfsrodb.C
  u_int32_t nfh = (fsinfo.blocksize - 100) / (20*2);

  if (block < SFSRO_NDIR) {
    ID = sfshash_to_chordid (&(inode->direct[block]));
    (*cb) (ID, true);
  }
  else if (block < SFSRO_NDIR + nfh) {
    ID = sfshash_to_chordid (&(inode->indirect));
    unsigned int slotno1 = block - SFSRO_NDIR;
    bmap_recurse (pfonly, cb, slotno1, ID, true);
  }
  else if (block < SFSRO_NDIR + nfh * nfh) {
    unsigned int slotno1 = (block - SFSRO_NDIR) / nfh;
    unsigned int slotno2 = (block - SFSRO_NDIR) % nfh;

    ID = sfshash_to_chordid (&(inode->double_indirect));
    bmap_recurse (pfonly, wrap (this, &chord_server::bmap_recurse, pfonly, cb, slotno2),
		  slotno1, ID, true);
  }
  else if (block < SFSRO_NDIR + nfh * nfh * nfh) {
    unsigned int slotno1 = (block - SFSRO_NDIR) / (nfh * nfh);
    unsigned int slotno2 = ((block - SFSRO_NDIR) % (nfh * nfh)) / nfh;
    unsigned int slotno3 = (block - SFSRO_NDIR) % nfh;

    ID = sfshash_to_chordid (&(inode->triple_indirect));
    bmap_recurse (pfonly, wrap (this, &chord_server::bmap_recurse, pfonly,
				wrap (this, &chord_server::bmap_recurse, pfonly,
				      cb, slotno3), slotno2), slotno1, ID, true);
  }
  else {
    warn << "bmap: block out of range: " << block << "\n";
    (*cb) (0, false);
  }
}

void
chord_server::bmap_recurse(bool pfonly, cbbmap_t cb, u_int32_t slotno, chordID ID, bool success)
{
  if (success) {
    fetch_data (pfonly, ID,
		wrap (this, &chord_server::bmap_recurse_data_cb, pfonly, cb, slotno));
  } else {
    (*cb) (0, false);
  }
}

void
chord_server::bmap_recurse_data_cb (bool pfonly, cbbmap_t cb,
				    u_int32_t slotno, ptr<sfsro_data> dat)
{
  if (dat) {
    assert (dat->type == SFSRO_INDIR);
    sfsro_indirect *indirect =  dat->indir;
    sfs_hash *fh = &indirect->handles[slotno];
    chordID ID =  sfshash_to_chordid (fh);
    (*cb) (ID, true);
  } else {
    (*cb) (0, false);
  }
}





// ------------------- read block from file (or directory) --------------------

// XXX should this call check the type of the block? 
// XXX the ref counting is questionable here.. 
void
chord_server::read_file_data (bool pfonly, size_t block, sfsro_inode_reg *inode, cbdata_t cb)
{
  // the caller must ensure that block is within the size of the file

  // convert logical file block to chordID
  bmap (pfonly, block, inode, wrap (this, &chord_server::read_file_data_bmap_cb, pfonly, cb));
}



void
chord_server::read_file_data_bmap_cb (bool pfonly, cbdata_t cb, chordID ID, bool success)
{
  if (success) {
    fetch_data (pfonly, ID, cb);
  } else {
    (*cb) (NULL);
  }
}


// ------------------------ raw data fetch ------------------

// pfonly:
//   -- if the data is cached, then call it back.
//   -- otherwise, ensure the data is read in (it might already be incoming),
//      but don't call it back.
//
void
chord_server::fetch_data (bool pfonly, chordID ID, cbdata_t cb)
{
  fetch_data (pfonly, ID, DHASH_CONTENTHASH, cb);
}

void
chord_server::fetch_data (bool pfonly, chordID ID, dhash_ctype ct, cbdata_t cb)
{
  if (ptr<sfsro_data> dat = data_cache [ID]) {
    (*cb) (dat);
  }
  else if (wait_list *l = pf_waiters[ID]) {
    if (!pfonly) {
      fetch_wait_state *w = New fetch_wait_state (cb);
      l->insert_head (w);
    }
  }
  else {
    pf_waiters.insert (ID);
    wait_list *l = pf_waiters[ID];
    fetch_wait_state *w = New fetch_wait_state (cb);
    l->insert_head (w);
    dh.retrieve (ID, ct, wrap (this, &chord_server::fetch_data_cb, ID, cb));
  }
}

void
chord_server::fetch_data_cb (chordID ID, cbdata_t cb, 
			     dhash_stat stat,
			     ptr<dhash_block> blk,
			     vec<chordID> path)
{
  ptr<sfsro_data> data = NULL;

  if (blk) {
    data = New refcounted<sfsro_data>;
    xdrmem x (blk->data, blk->len, XDR_DECODE);
    if (xdr_sfsro_data (x.xdrp (), data)) {
      data_cache.insert (ID, data);
    } else {
      warn << "Couldn't unmarshall data\n";
    }
  }

  wait_list *l = pf_waiters[ID];
  assert (l); 
  
  while (fetch_wait_state *w = l->first) {
    (*w->cb) (data);
    l->remove (w);
    delete w;
  }
  
  pf_waiters.remove (ID);
}
