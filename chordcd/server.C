// KNOWN ISSUES:
//   * READDIR/READDIRP
//     - does not deal with directories bigger than one block
//     - getting the fileid (ie. inode) for ".." requires a namei() lookup (ie. expensive)
//   * READ
//     - does not deal with request which span blocks
//     - pre-fetching not implemented
//   * LOOKUP
//     - linear scan algo could be improved, to return NOENT
//       when it is sure that the entry does not exists
//     - or binary lookup could be implemented
//   * GENERAL
//     - get_data() don't re-issue request for block x, if x has a pending read
//     - embedded inodes to improve LOOKUP performance
//     - can make lots of optimizations to filesystem structure which are easy when
//       the FS is read-only, but hard in a read-write filesystem, namely embedded inodes
//     - currently never evicts anything from the data_cache
//     - need to think more about mappings between, fileid, nfs_fh, sfs_hash, chordID
//   * XXX
//     - search for this and find more


#include "chordcd.h"
#include "rxx.h"
#include "arpc.h"
#include "xdr_suio.h"
#include "afsnode.h"

#define RPCTRACE 0
#define RPCTRACE_LOOKUP 0

#define LSD_SOCKET "/tmp/chord-sock"
#define CMTU 1024

static void splitpath (vec<str> &out, str in);
void ro2nfsattr (sfsro_inode *si, fattr3 *ni, chordID ID);
static bool xdr_putentry3 (XDR *x, u_int64_t ino, 
			   filename name, u_int32_t cookie);
static int readdir_xdr (XDR *x, void *_uio);
static void parentpath (str &parent, str &filename, const str inpath);



chord_server::chord_server (u_int cache_maxsize)
  : data_cache (cache_maxsize)
{
  int fd = unixsocket_connect (LSD_SOCKET);
  if (fd < 0) 
    fatal << "Failed to bind to lsd control socket on " << LSD_SOCKET << "\n";
  cclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
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
    fetch_data (ID, wrap (this, &chord_server::getroot_fh, rfh_cb));
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
    this->rootdirID = d->fsinfo->info.rootfh;
    this->fsinfo = *d->fsinfo;
    fetch_data (rootdirID, wrap (this, &chord_server::getrootdir_cb, rfh_cb));
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
#if RPCTRACE
    warn << "** NFSPROC3_READLINK\n";
#endif
    nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
    chordID fh = nfsfh_to_chordid (nfh);
    fetch_data (fh,wrap (this, &chord_server::readlink_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_GETATTR: 
    {
#if RPCTRACE
      warn << "** NFSPROC3_GETATTR\n";
#endif
      nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh,wrap (this, &chord_server::getattr_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_FSSTAT:
    {
#if RPCTRACE
      warn << "** NFSPROC3_FSSTAT\n";
#endif
      fsstat3res res (NFS3_OK);
      rpc_clear (res);
      sbp->reply (&res);
    }
    break;
  case NFSPROC3_FSINFO:
    {
#if RPCTRACE
      warn << "** NFSPROC3_FSINFO\n";
#endif
      int blocksize = fsinfo.info.blocksize;
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
#if RPCTRACE
      warn << "** NFSPROC3_ACCESS\n";
#endif
      access3args *aa = sbp->template getarg<access3args> ();
      nfs_fh3 *nfh = &(aa->object);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh, wrap (this, &chord_server::access_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_LOOKUP: 
    {
#if RPCTRACE
      warn << "** NFSPROC3_LOOKUP\n";
#endif
      diropargs3 *dirop = sbp->template getarg<diropargs3> ();

#if RPCTRACE_LOOKUP
    print_diropargs3 (dirop, NULL, 0, "", "LK  ");
#endif



      nfs_fh3 *nfh = &(dirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh, wrap (this, &chord_server::lookup_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READDIR:
    {
#if RPCTRACE
      warn << "** NFSPROC3_READDIR\n";
#endif
      readdir3args *readdirop = sbp->template getarg<readdir3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh, wrap (this, &chord_server::readdir_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READDIRPLUS:
    {
#if RPCTRACE
      warn << "** NFSPROC3_READDIRPLUS\n";
#endif
      readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh, wrap (this, &chord_server::readdirp_inode_cb, sbp, fh));
    }
    break;
  case NFSPROC3_READ:
    {
#if RPCTRACE
      warn << "** NFSPROC3_READ\n";
#endif
      read3args *ra = sbp->template getarg<read3args> ();
      nfs_fh3 *nfh = &(ra->file);
      chordID fh = nfsfh_to_chordid (nfh);
      fetch_data (fh, wrap (this, &chord_server::read_inode_cb, sbp, fh));
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
    fattr3 fa;
    ro2nfsattr (data->inode, &fa, ID);
    *nfsres.attributes = fa;

#if RPCTRACE
    print_getattr3res (&nfsres, NULL, 0, "", "GA  ");
#endif
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
    
#if RPCTRACE
    print_access3res (&nfsres, NULL, 0, "", "AC  ");
#endif
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
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ) {
    sbp->error (NFS3ERR_NOTDIR);
  } else {
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
    
    fattr3 fa;
    ro2nfsattr (dirdata->inode, &fa, dirID);
    *nfsres->resok->dir_attributes.attributes = fa;
    
    nfs_fh3 nfh;
    chordid_to_nfsfh (&dataID, &nfh);
    
    nfsres->resok->object = nfh;
    nfsres->resok->obj_attributes.set_present (true);
    
    fattr3 ofa;
    ro2nfsattr (data->inode, &ofa, dataID);
    *nfsres->resok->obj_attributes.attributes = ofa;
    
#if RPCTRACE_LOOKUP
    print_lookup3res (nfsres, NULL, 0, "", "LK  ");
#endif
    sbp->reply (nfsres);
  } else {
    sbp->error (status);
  }
}


/* Given a path, split it into two pieces:
   the parent directory path and the filename.

   Examples:

   path      parent   filename
   "/"       "/"      ""
   "/a"      "/"      "a"
   "/a/"     "/"      "a"
   "/a/b"    "/a"     "b"
   "/a/b/c"  "/a/b"   "c"
*/
static void
parentpath (str &parent, str &filename, const str inpath)
{
  vec<str> ppv;
  parent = str ("/");
  filename = str ("");

  splitpath (ppv, inpath);

  if (ppv.size () == 0)
    return;

  filename = ppv.pop_back ();
  if (ppv.size () == 0)
    return;

  // What a non-intuitive way to do concatenation!
  parent = strbuf () << "/" << join (str("/"), ppv);
}


void
chord_server::readdir_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ) {
    sbp->error (NFS3ERR_NOTDIR);
  } else {
    readdir3args *nfsargs = sbp->template getarg<readdir3args> ();

    // cookie:
    //   - high 32 bits are the block number
    //   - low 32 bits are
    
    u_int64_t cookie = nfsargs->cookie;
    u_int32_t blockno = cookie >> 32;
    u_int32_t entryno = (u_int32_t)cookie;

    if (blockno == 0 && entryno <= 1) {
      read_file_data (blockno, data->inode->reg, false,
		      wrap (this, &chord_server::readdir_dotdot_data_cb, sbp, data, ID));
    } else {
      read_file_data (blockno, data->inode->reg, false,
		      wrap (this, &chord_server::readdir_fetch_dirdata_cb, sbp, data, ID, 0));
    }
  }
}

void
chord_server::readdir_dotdot_data_cb(nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID,
				     ptr<sfsro_data> dirblk)
{
  // dirdata -- the directory inode
  // dirID -- hash(dirdata) (ie. its chordID)
  // d_dat -- is the first data block of the directory

  if (dirblk == NULL) {
    sbp->error (NFS3ERR_IO);
  } else if (dirblk->type != SFSRO_DIRBLK) {
    sbp->error (NFS3ERR_IO);
  } else {
    str parent, filename;
    parentpath (parent, filename, dirblk->dir->path);
    namei (parent,
	   wrap (this, &chord_server::readdir_dotdot_namei_cb,
		 sbp, dirdata, dirID, dirblk));
  }

}

void
chord_server::readdir_dotdot_namei_cb(nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID, ptr<sfsro_data> dirblk,
				      ptr<sfsro_data> data, chordID dataID, nfsstat3 status)
{
  // dirdata -- the directory inode
  // dirID -- hash(dirdata) (ie. its chordID)
  // data -- the inode of the parent of 'dirdata'
  // dirblk -- the first block of data in the directory.


  if (data == NULL) {
    sbp->error (NFS3ERR_IO);
  } else if (data->type != SFSRO_INODE) {
    sbp->error (NFS3ERR_IO);
  } else {
    chordID parentID = dataID;
    readdir_fetch_dirdata_cb(sbp, dirdata, dirID, parentID, dirblk);
  }
}

void
chord_server::readdir_fetch_dirdata_cb (nfscall *sbp, ptr<sfsro_data> dirdata, chordID dirID, chordID parentID,
					ptr<sfsro_data> dirblk) 
{
  if (dirblk == NULL) {
      sbp->error (NFS3ERR_IO);
  } else if (dirblk->type != SFSRO_DIRBLK) {
      sbp->error (NFS3ERR_IO);
  } else {

    sfsro_directory *dir = dirblk->dir;
    readdir3args *nfsargs = sbp->template getarg<readdir3args> ();
    bool errors = false;
    
#if 0
    u_int64_t cookie = nfsargs->cookie;
    u_int32_t blockno = cookie >> 32;
    u_int32_t entryno = (u_int32_t)cookie;
#endif
    
    readdir3res *nfsres = sbp->template getres<readdir3res> ();
    rpc_clear (*nfsres);
    nfsres->set_status (NFS3_OK);
    nfsres->resok->dir_attributes.set_present (true);
    
    fattr3 fa;
    ro2nfsattr (dirdata->inode, &fa, dirID);
    *nfsres->resok->dir_attributes.attributes = fa;
    xdrsuio x (XDR_ENCODE, true);
    
    if (!xdr_putint (&x, NFS_OK))
      errors = true;
    
    if (!xdr_post_op_attr (&x, &nfsres->resok->dir_attributes) 
	|| !xdr_puthyper (&x, 0))
      errors = true;
    
    sfsro_dirent *roe = dir->entries;
    
    uint32 i = 0;
    while (roe && nfsargs->cookie > i) {
      roe = roe->nextentry;
      i++;
    }

    // (the .. fileid is not that easy to come by either..)
    if (!xdr_putentry3 (&x, dirID.getui(),  ".", 0) ||
	!xdr_putentry3 (&x, parentID.getui(),  "..", 0))
      errors = true;

    uint64 fileid;
    for (uint32 j = 0; 
	 roe && (xdr_getpos (&x) + 24 + roe->name.len () <= nfsargs->count);
	 j++) 
      {
	fh2fileid (&roe->fh, &fileid);
	if (!xdr_putentry3 (&x, fileid,  roe->name, nfsargs->cookie + j))
	  errors = true;
	roe = roe->nextentry;
      }
  
    if (!xdr_putint (&x, 0) 	// NULL entry *
	|| !xdr_putint (&x, !roe))	// bool eof
      errors = true;

    if (!errors)
      sbp->reply (x.uio (), &readdir_xdr);
    else
      sbp->error (NFS3ERR_IO);
  }
}




static bool
xdr_putentry3 (XDR *x, u_int64_t ino, filename name, u_int32_t cookie)
{
  return
    // entry * (non-null):
    xdr_putint (x, 1)
    // uint64 fileid:
    && xdr_puthyper (x, ino)
    // filename name:
    && xdr_filename (x, &name)
    // uint64 cookie:
    && xdr_puthyper (x, cookie);
}


static int
readdir_xdr (XDR *x, void *_uio)
{
  assert (x->x_op == XDR_ENCODE);
  suio *uio = static_cast<suio *> (_uio);
  xsuio (x)->take (uio);
  return true;
}


void
chord_server::readdirp_inode_cb (nfscall *sbp, chordID ID, ptr<sfsro_data> data)
{
  if (!data) 
    sbp->error (NFS3ERR_STALE);
  else if (data->type != SFSRO_INODE) 
    sbp->error (NFS3ERR_IO);
  else if (data->inode->type != SFSRODIR && data->inode->type != SFSRODIR_OPAQ) {
    sbp->error (NFS3ERR_NOTDIR);
  } else {
    fetch_data (sfshash_to_chordid(&data->inode->reg->direct[0]),
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
  
  fattr3 fa;
  ro2nfsattr (dirdata->inode, &fa, dirID);
  *nfsres->resok->dir_attributes.attributes = fa;
  
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
    fh2fileid (&roe->fh, &(*last)->fileid);
    (*last)->name = roe->name;
    
    // XXXX Make LINUX happy
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
  } else if (ra->offset >= data->inode->reg->size) {
    read3res nfsres(NFS3_OK);
    fattr3 fa;
    
    ro2nfsattr(data->inode, &fa, ID);
    nfsres.resok->count = 0;
    nfsres.resok->eof = 1;
    nfsres.resok->file_attributes.set_present(1);
    *nfsres.resok->file_attributes.attributes = fa;
    sbp->reply(&nfsres);
  }
  else {
    //XXX if a read straddles two blocks we will do a short read 
    ///   i.e. only read the first block
    size_t block = ra->offset / fsinfo.info.blocksize;
    read_file_data (block, data->inode->reg, false,
		    wrap (this, &chord_server::read_data_cb,
			  sbp, data, ID));

    // #define PF 8
    //     size_t pf_lim = block + PF;
    //    for (size_t b = block + 1; b < pf_lim ; b++)
    //      if ((b+1)*fsinfo.info.blocksize < data->inode->reg->size) 
    //	read_file_block (b, data->inode, true, wrap (&ignore_me));
  }
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
    
    fattr3 fa;
    ro2nfsattr(inode->inode, &fa, inodeID);
    
    unsigned int blocksize = (unsigned int)fsinfo.info.blocksize;
    
    size_t start = ra->offset % blocksize;
    size_t n = MIN (MIN (ra->count, size), size - start);
    
    nfsres.resok->count = n;
    nfsres.resok->eof = (fa.size >= ra->offset + n) ? 1 : 0;
    nfsres.resok->data.setsize(n);
    memset (nfsres.resok->data.base (), 'a', nfsres.resok->data.size ());
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

// XXX fix this up
unsigned int
chord_server::chordid_to_fileid (chordID &ID)
{
  return ID.getui();
}


chordID
chord_server::sfshash_to_chordid (sfs_hash *fh) 
{
  bigint n;
  mpz_set_rawmag_be (&n, fh->base (), fh->size ());
  return n;
}

void
chord_server::fh2fileid (sfs_hash *fh, uint64 *fileid)
{
  *fileid = gethyper (fh->base ());
}

void 
ro2nfsattr (sfsro_inode *si, fattr3 *ni, chordID ID)
{
  if (si->type == SFSROLNK) {
    ni->nlink = si->lnk->nlink;
    ni->size = si->lnk->dest.len ();
    ni->used = 0;
    ni->mtime = si->lnk->mtime;
    ni->ctime = si->lnk->ctime;
    ni->atime = si->lnk->mtime;

  } else {
    ni->nlink = si->reg->nlink;
    ni->size = si->reg->size;
    ni->used = si->reg->used;
    ni->mtime = si->reg->mtime;
    ni->ctime = si->reg->ctime;
    ni->atime = si->reg->mtime;
  }

  /* Below are synthesized attributes */
  ni->mode = 0444;
  
  switch (si->type) {
  case SFSROREG_EXEC:
    ni->mode = 0555;
  case SFSROREG:
    ni->type = NF3REG;
    break;
  case SFSRODIR:
    ni->mode = 0555;
    ni->type = NF3DIR;
    break;
  case SFSRODIR_OPAQ:
    ni->mode = 0111;
    ni->type = NF3DIR;
    break;
  case SFSROLNK:
    ni->type = NF3LNK;
    break;
  default:
    fatal ("server::ro2nfsattr: Unknown si->type %X\n",
	   si->type);
    break;
  }
    
  ni->uid = sfs_uid;
  ni->gid = sfs_gid;
  ni->rdev.minor = 0;
  ni->rdev.major = 0;
  ni->fsid = 0;
  ni->fileid = ID.getui();
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
    assert (dirdata->inode->type == SFSRODIR || dirdata->inode->type == SFSRODIR_OPAQ);
    dirinode = dirdata->inode->reg;

    curblock = 0;
    if (dirinode->direct.size() > 0)
      maxblock = (dirinode->direct.size() - 1)  / blocksize;
    else
      maxblock = -1; 
  }
};


void
chord_server::lookup(ptr<sfsro_data> dirdata, chordID dirID , str component, cblookup_t cb)
{
#if RPCTRACE_LOOKUP
  warn << "looking in dirID=" << dirID << " for '" << component << "'\n";
#endif

  if (!dirdata) {
    (*cb) (NULL, 0, NFS3ERR_STALE);
  } else if (dirdata->type != SFSRO_INODE) {
    (*cb) (NULL, 0, NFS3ERR_IO);
  } else if (dirdata->inode->type != SFSRODIR && 
	     dirdata->inode->type != SFSRODIR_OPAQ) {  
    (*cb) (NULL, 0, NFS3ERR_IO);    
  } else if (component == ".") {
    (*cb) (dirdata, dirID, NFS3_OK);
  } else {
    ref<lookup_state> st = New refcounted<lookup_state> (dirdata, component, cb, fsinfo.info.blocksize);
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
    read_file_data (curblock, st->dirinode, false,
    		    wrap (this, &chord_server::lookup_scandir_nextblock_cb, st));
  }
}


void
chord_server::lookup_scandir_nextblock_cb(ref<lookup_state> st, ptr<sfsro_data> dat)
{
  if (dat == NULL || dat->type != SFSRO_DIRBLK) 
    (*st->cb) (NULL, 0, NFS3ERR_STALE);
  else {
    sfsro_directory *dir = dat->dir;

#if RPCTRACE_LOOKUP
    warn << "cur " << st->curblock << " max " << st->maxblock << "\n";
    print_sfsro_directory (dir, NULL, 0, "", "LK.DIRBLK  ");
#endif    

    if (st->component == "..") { 
      namei (dat->dir->path, st->cb);
    } else {
      sfsro_dirent *dirent = dirent_lookup (st->component, dir);
      if (dirent) {
	chordID componentID = sfshash_to_chordid (&dirent->fh);
	fetch_data (componentID, wrap (this, &chord_server::lookup_component_inode_cb, st, componentID));
      } else {
	lookup_scandir_nextblock(st);
      }
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
chord_server::bmap(size_t block, sfsro_inode_reg *inode, cbbmap_t cb)
{
  chordID ID;

  // XXX this is the same calculation that is at the end of sfsrodb/sfsrodb.C
  u_int32_t nfh = (fsinfo.info.blocksize - 100) / (20*2);

  if (block < SFSRO_NDIR) {
    ID = sfshash_to_chordid (&(inode->direct[block]));
    (*cb) (ID, true);
  }
  else if (block < SFSRO_NDIR + nfh) {
    ID = sfshash_to_chordid (&(inode->indirect));
    unsigned int slotno1 = block - SFSRO_NDIR;
    bmap_recurse (cb, slotno1, ID, true);
  }
  else if (block < SFSRO_NDIR + nfh * nfh) {
    unsigned int slotno1 = (block - SFSRO_NDIR) / nfh;
    unsigned int slotno2 = (block - SFSRO_NDIR) % nfh;

    ID = sfshash_to_chordid (&(inode->double_indirect));
    bmap_recurse (wrap (this, &chord_server::bmap_recurse, cb, slotno2), slotno1, ID, true);
  }
  else if (block < SFSRO_NDIR + nfh * nfh * nfh) {
    unsigned int slotno1 = (block - SFSRO_NDIR) / (nfh * nfh);
    unsigned int slotno2 = ((block - SFSRO_NDIR) % (nfh * nfh)) / nfh;
    unsigned int slotno3 = (block - SFSRO_NDIR) % nfh;

    ID = sfshash_to_chordid (&(inode->triple_indirect));
    bmap_recurse (wrap (this, &chord_server::bmap_recurse,
			wrap (this, &chord_server::bmap_recurse, cb, slotno3),
			slotno2),
		  slotno1, ID, true);
  }
  else {
    warn << "bmap: block out of range: " << block << "\n";
    (*cb) (0, false);
  }
}

void
chord_server::bmap_recurse(cbbmap_t cb, unsigned int slotno, chordID ID, bool success)
{
  if (success) {
    fetch_data (ID, wrap (this, &chord_server::bmap_recurse_data_cb, cb, slotno));
  } else {
    (*cb) (0, false);
  }
}

void
chord_server::bmap_recurse_data_cb(cbbmap_t cb, unsigned int slotno, ptr<sfsro_data> dat)
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
chord_server::read_file_data (size_t block, sfsro_inode_reg *inode, bool pfonly,
			      cbgetdata_t cb)
{
  // the caller must ensure that block is within the size of the file

  // convert logical file block to chordID
  bmap(block, inode, wrap (this, &chord_server::read_file_data_bmap_cb, cb, pfonly));
}


void
chord_server::read_file_data_bmap_cb (cbgetdata_t cb, bool pfonly, chordID ID, bool success)
{
  if (success) {
    get_data (ID, cb, pfonly);
  } else {
    (*cb) (NULL);
  }
}



// ------------------------ raw data fetch ------------------

void
chord_server::fetch_data (chordID ID, cbfetch_block_t cb)
{
  get_data (ID, cb, false);
}


void
chord_server::get_data (chordID ID, cbgetdata_t cb, bool pf_only) 
{

  //check for pending request for the same data
  wait_list *l;
  if ( (l = pf_waiters[ID]) ) {
    if (!pf_only) { //pre-fetching in-flight data --> NOP
      fetch_wait_state *w = New fetch_wait_state (cb);
      l->insert_head (w);
    }
    return;
  }

  //check cache
  ptr<sfsro_data> dat;
  if ((dat = data_cache [ID])) {
    (*cb) (dat);
    return;
  }


  dhash_fetch_arg arg;
  arg.key = ID;
  arg.len = CMTU;
  arg.start = 0;
  
  dhash_res *res = New dhash_res (DHASH_OK);

  cclnt->call (DHASHPROC_LOOKUP, &arg, res, 
	       wrap (this, &chord_server::get_data_initial_cb, res, cb, ID)); 
}


void
chord_server::get_data_initial_cb (dhash_res *res, cbgetdata_t cb, chordID ID,
				   clnt_stat err) 
{
  
  if (err) 
    fatal << "EOF from chord daemon. Shutting down\n";
  else if (res->status != DHASH_OK) {
    warn << "error fetching " << ID << "\n";
    finish_getdata (NULL, 0, cb, bigint (0));
  } else if (res->resok->attr.size == res->resok->res.size ()) {
    finish_getdata (res->resok->res.base (), res->resok->res.size (), 
		    cb, ID);
  } else {
    char *buf = New char[res->resok->attr.size];
    memset (buf, 'a', res->resok->attr.size);
    memcpy (buf, res->resok->res.base (), res->resok->res.size ());
    unsigned int offset = res->resok->res.size ();
    unsigned int *read = New unsigned int (offset);
    while (offset < res->resok->attr.size) {
      ptr<dhash_transfer_arg> arg = New refcounted<dhash_transfer_arg> ();
      arg->farg.key = ID;
      arg->farg.len = (offset + CMTU < res->resok->attr.size) ? CMTU :
	res->resok->attr.size - offset;
      arg->source = res->resok->source;
      arg->farg.start = offset;

      dhash_res *new_res = New dhash_res (DHASH_OK);
      
      // XXX what would happen if DHASH had cached the block
      // so that get_data_partial_cb happened right away,
      // ie. without offset being incremented??
      //
      cclnt->call (DHASHPROC_TRANSFER, arg, new_res,
		   wrap (this, &chord_server::get_data_partial_cb, 
			 new_res, buf, read, cb, ID));
      offset += arg->farg.len;
    }
  }
  delete res;
}

void
chord_server::get_data_partial_cb (dhash_res *res, char *buf, 
				   unsigned int *read,
				   cbgetdata_t cb,
				   chordID ID,
				   clnt_stat err) 
{
  if (err)
    fatal << "EOF from chord daemon. Shutting down\n";
  else if (res->status != DHASH_OK) {
    delete buf;
    delete read;
    // 
    // XXX the call to get_data() is expecting its callback to be
    //     called back exactly once.  This code could callback once
    //     for every chunk of the block.
    //
    assert (0); 
    (*cb) (NULL);
    return;
  } else {
    memcpy (buf + res->resok->offset, res->resok->res.base (),
	    res->resok->res.size ());
    *read += res->resok->res.size ();
  }

  if (*read == res->resok->attr.size) {
    finish_getdata (buf, res->resok->attr.size, cb, ID);
    delete read;
  }

  delete res;
}  


void
chord_server::finish_getdata (char *buf, unsigned int size, cbgetdata_t cb,
			      chordID ID) 
{
  if (!buf) {
    warn << "error fetching " << ID << "(null) \n";
    (*cb)(NULL);
  } else {
    // XXX - check hash
    ptr<sfsro_data> data = New refcounted<sfsro_data>;
    xdrmem x (buf, size, XDR_DECODE);
    if (!xdr_sfsro_data (x.xdrp (), data)) {
      warn << "Couldn't unmarshall data\n";
      delete buf;
      (*cb)(NULL);
    } else {
      (*cb)(data);

      // add to cache
      data_cache.insert (ID, data);
      
      // XXX - check prefetch list
      wait_list *l;
      if ( (l = pf_waiters[ID]) ) {
	fetch_wait_state *w = l->first;
	while (w) {
	  (*w->cb) (data);
	  fetch_wait_state *next = l->next (w);
	  l->remove (w);
	  w = next;
	}
      }
      
      delete buf;
    }
  }
}

