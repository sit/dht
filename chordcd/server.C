#include "chordcd.h"
#include "rxx.h"
#include "arpc.h"
#include "xdr_suio.h"
#include "afsnode.h"

#define LSD_SOCKET "/tmp/chord-sock"
#define MTU 1024

void ro2nfsattr (sfsro_inode *si, fattr3 *ni, chordID ID);
static bool xdr_putentry3 (XDR *x, u_int64_t ino, 
			   filename name, u_int32_t cookie);
static int readdir_xdr (XDR *x, void *_uio);

chord_server::chord_server () 
{
  int fd = unixsocket_connect (LSD_SOCKET);
  if (fd < 0) 
    fatal << "Failed to bind to lsd control socket on " << LSD_SOCKET << "\n";
  cclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
}

void
chord_server::setrootfh (str root, callback<void, nfs_fh3 *>::ref rfh_cb) 
{
  warn << "root is " << root << "\n";
  //                       chord:ade58209f
  static rxx path_regexp ("chord:([0-9a-zA-Z]*)$"); 

  if (!path_regexp.search (root)) (*rfh_cb) (NULL);
  str rootfhstr = path_regexp[1];
  str dearm = dearmor64A (rootfhstr);
  chordID rootfh;
  mpz_set_rawmag_be (&rootfh, dearm.cstr (), dearm.len ());
  warn << "root file handle is " << rootfh << "\n";

  //fetch the root file handle too..
  get_data (rootfh, wrap (this, &chord_server::getroot_fh, rfh_cb), false);
}

void
chord_server::getroot_fh (callback<void, nfs_fh3 *>::ref rfh_cb, sfsro_data *d) 
{
 
  if (!d || (d->type  != CFS_FSINFO)) {
    if (!d) warn << "root block was null\n";
    (*rfh_cb) (NULL);
  } else {
    // XXX verify file handle
    warn << "returning true\n";
    fsinfo = *d->fsinfo;
    nfs_fh3 *fh = New nfs_fh3;
    chordid_to_nfsfh (&d->fsinfo->info.rootfh, fh);
    (*rfh_cb) (fh);
  }
}

void
chord_server::dispatch (ref<nfsserv> ns, nfscall *sbp)
{

  warn << "dispatching " << sbp->proc () << "\n";
  switch(sbp->proc()) {
  case NFSPROC3_READLINK:
    {
    nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
    chordID fh = nfsfh_to_chordid (nfh);
    inode_lookup (fh,wrap (this, &chord_server::readlink_fetch_inode, sbp, fh));
    }
    break;
  case NFSPROC3_GETATTR: 
    {
      nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh,wrap (this, &chord_server::getattr_fetch_inode, sbp, fh));
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
  case NFSPROC3_LOOKUP: 
    {
      diropargs3 *dirop = sbp->template getarg<diropargs3> ();
      nfs_fh3 *nfh = &(dirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh, wrap (this, &chord_server::lookup_fetch_dirinode,sbp));
    }
    break;
  case NFSPROC3_ACCESS:
    {
      access3args *aa = sbp->template getarg<access3args> ();
      nfs_fh3 *nfh = &(aa->object);
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh, wrap (this, &chord_server::access_fetch_inode, 
			      sbp, fh));

    }
    break;
  case NFSPROC3_READ:
    {
      read3args *ra = sbp->template getarg<read3args> ();
      nfs_fh3 *nfh = &(ra->file);
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh, wrap (this, &chord_server::read_fetch_inode, 
			      sbp, fh));
    }
    break;
  case NFSPROC3_READDIR:
    {
      readdir3args *readdirop = sbp->template getarg<readdir3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh, wrap (this, &chord_server::readdir_fetch_inode, 
			      sbp, fh));
    }
    break;
  case NFSPROC3_READDIRPLUS:
    {
      readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();
      nfs_fh3 *nfh = &(readdirop->dir);
      chordID fh = nfsfh_to_chordid (nfh);
      inode_lookup (fh, wrap (this, &chord_server::readdirp_fetch_inode, 
			      sbp, fh));
    }
    break;
  case NFSPROC3_NULL:
    {
      sbp->reply (NULL);
      return;
    }
    break;
  case NFSPROC_CLOSE:
    warn << "close?\n";
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
chord_server::getattr_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *i) 
{
  if (!i) sbp->error (NFS3ERR_STALE);
  else {
    getattr3res nfsres (NFS3_OK);
    fattr3 fa;
    ro2nfsattr (i, &fa, ID);
    *nfsres.attributes = fa;
    sbp->reply (&nfsres);
  }
  return;
}

void
chord_server::readlink_fetch_inode (nfscall *sbp, chordID, sfsro_inode *ip)
{
  if (!ip) sbp->error (NFS3ERR_STALE);
  else {
    if (ip->type != SFSROLNK) 
      sbp->error (NFS3ERR_INVAL);
    else if (ip->lnk->dest.len () <= 0)
      sbp->error (NFS3ERR_INVAL);   // ?????
    else if (ip->lnk->dest.len () >= (unsigned int)fsinfo.info.blocksize)
      sbp->error (NFS3ERR_NOTSUPP);   // XXX
    else {
      readlink3res nfsres (NFS3_OK);
      
      nfsres.resok->data = ip->lnk->dest;
      sbp->reply(&nfsres);
      
    }
  }
}

void
chord_server::lookup_fetch_dirinode (nfscall *sbp, sfsro_inode *dir_i) 
{
  if (!dir_i) sbp->error (NFS3ERR_STALE);
  else { 
    if (dir_i->type != SFSRODIR && dir_i->type != SFSRODIR_OPAQ) 
      sbp->error (NFS3ERR_NOTDIR);
    else if (dir_i->reg->direct.size() <= 0) {
      warn << "returning NOENT in lookupinode+lookupres\n";
      sbp->error (NFS3ERR_NOENT);
    } else 
      get_data (sfshash_to_chordid (&dir_i->reg->direct[0]),
		wrap (this, &chord_server::lookup_fetch_dirblock,
		      sbp, dir_i), false);
    
  }
}

void
chord_server::lookup_fetch_dirblock (nfscall *sbp, sfsro_inode *dir_inode,
				     sfsro_data *dir_dat) 
{
  assert (dir_dat->type == SFSRO_DIRBLK);
  diropargs3 *dirop = sbp->template getarg<diropargs3> ();
  nfs_fh3 *nfh = &(dirop->dir);
  chordID dir_fh = nfsfh_to_chordid (nfh);
  sfsro_directory *dir = dir_dat->dir;

  if (dirop->name == "."  
      || (dirop->name == ".." && dir->path.len () == 0))
    {
      //dir is object
      lookup_fetch_obj_inode (sbp, dir_fh, dir_inode, dir_fh, dir_inode);
    }
  else if (dirop->name == "..")
    {
      warn << ".. not implemented\n";
      sbp->error (NFS3ERR_STALE);
    } else {
      sfsro_dirent *dirent = dirent_lookup(&(dirop->name), dir);
      
      if (dirent == NULL) {
	///// XXX do we need to send post_op_attr if errors?
	sbp->error (NFS3ERR_NOENT);
      } else {
	chordID obj_fh = sfshash_to_chordid (&dirent->fh);
	inode_lookup (obj_fh, 
		      wrap (this, &chord_server::lookup_fetch_obj_inode, 
			    sbp, dir_fh, dir_inode, obj_fh));
      }
    }
}

void
chord_server::lookup_fetch_obj_inode (nfscall *sbp,
				      chordID dir_fh,
				      sfsro_inode *dir_inode,
				      chordID obj_fh,
				      sfsro_inode *obj_inode) 
{
  lookup3res *nfsres = sbp->template getres<lookup3res> ();
  nfsres->set_status (NFS3_OK);
  nfsres->resok->dir_attributes.set_present (true);

  fattr3 fa;
  ro2nfsattr (dir_inode, &fa, dir_fh);
  *nfsres->resok->dir_attributes.attributes = fa;

  nfs_fh3 nfh;
  chordid_to_nfsfh (&obj_fh, &nfh);
  
  nfsres->resok->object = nfh;
  nfsres->resok->obj_attributes.set_present (true);

  fattr3 ofa;
  ro2nfsattr (obj_inode, &ofa, obj_fh);
  *nfsres->resok->obj_attributes.attributes = ofa;

  sbp->reply (nfsres);

}

void
chord_server::access_fetch_inode (nfscall *sbp, chordID ID, sfsro_inode *i) 
{
  access3args *aa = sbp->template getarg<access3args> ();
  uint32 access_req = aa->access;
  access3res nfsres (NFS3_OK);
  
  nfsres.resok->access = access_check (i, access_req);

  sbp->reply (&nfsres);
}

void
chord_server::read_fetch_inode (nfscall *sbp, chordID ID,sfsro_inode *f_ip) 
{
  read3args *ra = sbp->template getarg<read3args> ();

  if (!(f_ip->type == SFSROREG
	|| f_ip->type == SFSROREG_EXEC 
	|| f_ip->type == SFSROLNK))
    sbp->error (NFS3ERR_IO);
  else if (ra->offset >= f_ip->reg->size) {
    read3res nfsres(NFS3_OK);
    fattr3 fa;
    
    ro2nfsattr(f_ip, &fa, ID);
    nfsres.resok->count = 0;
    nfsres.resok->eof = 1;
    nfsres.resok->file_attributes.set_present(1);
    *nfsres.resok->file_attributes.attributes = fa;
    sbp->reply(&nfsres);
  } else {
    //XXX if a read straddles two blocks we will do a short read 
    ///   i.e. only read the first block
    size_t block = ra->offset / fsinfo.info.blocksize;
    read_file_block (block, f_ip, false, wrap (this, 
					     &chord_server::read_fetch_block,
					     sbp, f_ip, ID));

    // XXX prefetch here
  }

}

void
chord_server::read_fetch_block (nfscall *sbp, sfsro_inode *f_ip, chordID ID,
				char *data, size_t size)
{
  read3args *ra = sbp->template getarg<read3args> ();
  read3res nfsres (NFS3_OK);

  fattr3 fa;
  ro2nfsattr(f_ip, &fa, ID);

  unsigned int blocksize = (unsigned int)fsinfo.info.blocksize;
  size_t start = ra->offset % blocksize;
  size_t n = MIN (MIN (ra->count, blocksize), size - start);
  nfsres.resok->count = n;
  nfsres.resok->eof = (fa.size >= ra->offset + n) ? 1 : 0;
  nfsres.resok->data.setsize(n);
  memcpy (nfsres.resok->data.base(), data + start, 
	  nfsres.resok->data.size ()); 
  nfsres.resok->file_attributes.set_present(1);
  *nfsres.resok->file_attributes.attributes = fa;
  sbp->reply (&nfsres);
}

void
chord_server::readdir_fetch_inode(nfscall *sbp, chordID ID, sfsro_inode *d_ip)
{
  if (!d_ip)
    sbp->error (NFS3ERR_NOENT);
  else if (d_ip->type != SFSRODIR && d_ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else {
    get_data (sfshash_to_chordid(&d_ip->reg->direct [0]), 
	      wrap (this, &chord_server::readdir_fetch_dir_data, 
		    sbp, d_ip, ID), false);
  }
}

void
chord_server::readdir_fetch_dir_data (nfscall *sbp, sfsro_inode *d_ip,
				      chordID ID,
				      sfsro_data *d_dat) 
{
  sfsro_directory *dir = d_dat->dir;
  readdir3args *nfsargs = sbp->template getarg<readdir3args> ();
  bool errors = false;

  readdir3res *nfsres = sbp->template getres<readdir3res> ();
  rpc_clear (*nfsres);
  nfsres->set_status (NFS3_OK);
  nfsres->resok->dir_attributes.set_present (true);
  
  fattr3 fa;
  ro2nfsattr (d_ip, &fa, ID);
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

void
chord_server::readdirp_fetch_inode(nfscall *sbp, chordID ID, sfsro_inode *d_ip)
{
  if (!d_ip)
    sbp->error (NFS3ERR_NOENT);
  else if (d_ip->type != SFSRODIR && d_ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else {
    get_data (sfshash_to_chordid(&d_ip->reg->direct [0]), 
	      wrap (this, &chord_server::readdirp_fetch_dir_data, 
		    sbp, d_ip, ID), false);
  }

}

void
chord_server::readdirp_fetch_dir_data (nfscall *sbp, sfsro_inode *d_ip,
				       chordID ID,
				       sfsro_data *d_dat) 
{
  sfsro_directory *dir = d_dat->dir;
  readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();
  
  readdirplus3res *nfsres = sbp->template getres<readdirplus3res> ();
  rpc_clear (*nfsres);
  nfsres->set_status (NFS3_OK);
  nfsres->resok->dir_attributes.set_present (true);
  
  fattr3 fa;
  ro2nfsattr (d_ip, &fa, ID);
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

// ------------------------ util -------------------
void
chord_server::inode_lookup (chordID fh, cbinode_t cb)
{
  get_data (fh, wrap (this, &chord_server::inode_lookup_fetch_cb, cb), false);
}

void
chord_server::inode_lookup_fetch_cb (cbinode_t cb, sfsro_data *dat)
{
  if (!dat) (*cb) (NULL);
  else {
    assert (dat->type == SFSRO_INODE);
    (*cb) (dat->inode);
  }
}

void
chord_server::read_file_block (size_t block, sfsro_inode *f_ip, bool pfonly,
			       cbblock_t cb) 
{
  if (block < SFSRO_NDIR) {
    chordID bid = sfshash_to_chordid (&(f_ip->reg->direct[block]));
    get_data (bid, wrap (this, &chord_server::read_fblock_cb, cb), pfonly);
  }
  else {
    warn << "no support for indirect blocks yet\n";
    (*cb) (NULL, 0);
  }

}

void
chord_server::read_fblock_cb (cbblock_t cb, sfsro_data *dat)
{
  if (dat) {
    assert (dat->type == SFSRO_FILEBLK);
    (*cb) (dat->data->base (), dat->data->size ());
  } else 
    (*cb) (NULL, 0);

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
  warn << "fh is " << raw.len () << " bytes long v. " << NFS3_FHSIZE - 4 << "\n";
  memcpy (nfh->data.base (), raw.cstr (), raw.len ());
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
  ni->fileid = ID.getui (); // XXX use all 64 bits

 
}

sfsro_dirent *
chord_server::dirent_lookup (filename3 *name, 
			     sfsro_directory *dir)
{
  assert (*name != ".");
  assert (*name != "..");

  sfsro_dirent *e = NULL, *e_prev = NULL;

  for (e = e_prev = dir->entries; e; e = e->nextentry) {

     if (*name == e->name)
	 return e;

     if ((e_prev->name < *name) &&
	 (*name < e->name))
	 return NULL;
     e_prev = e;
  }
  return NULL;
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

// ------------------------ block fetch ------------------
void
chord_server::get_data (chordID ID, cbgetdata_t cb, bool pf_only) 
{

  warn << "get_data " << ID << "\n";

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
  sfsro_data *dat;
  if ((dat = data_cache [ID])) {
    (*cb) (dat);
    return;
  }

  warn << ID << " not in cache\n";

  dhash_fetch_arg arg;
  arg.key = ID;
  arg.len = MTU;
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
    warn << " block was smaller than MTU\n";
    finish_getdata (res->resok->res.base (), res->resok->res.size (), 
		    cb, ID);
  } else {
    char *buf = New char[res->resok->attr.size];
    memcpy (buf, res->resok->res.base (), res->resok->res.size ());
    unsigned int offset = res->resok->res.size ();
    unsigned int *read = New unsigned int (offset);
    while (offset < res->resok->attr.size) {
      ptr<dhash_transfer_arg> arg = New refcounted<dhash_transfer_arg> ();
      arg->farg.key = ID;
      arg->farg.len = (offset + MTU < res->resok->attr.size) ? MTU:
	res->resok->attr.size;
      arg->source = res->resok->source;
      arg->farg.start = offset;

      dhash_res *new_res = New dhash_res (DHASH_OK);
      
      warn << "asking for " << arg->farg.len << " at " << offset << "\n";
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
    (*cb) (NULL);
  } else {
    memcpy (buf + res->resok->offset, res->resok->res.base (),
	    res->resok->res.size ());
    *read += res->resok->res.size ();
  }

  if (*read == res->resok->attr.size) {
    finish_getdata (buf, res->resok->attr.size, cb, ID);
    delete buf;
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
    sfsro_data *data = New sfsro_data;
    xdrmem x (buf, size, XDR_DECODE);
    if (!xdr_sfsro_data (x.xdrp (), data)) {
      warn << "Couldn't unmarshall data\n";
      (*cb)(NULL);
    } else {
      warn << "returning non-null (" << (int)data << ")\n";
      (*cb)(data);
      // XXX - check prefetch list
      // add to cache
      data_cache.insert (ID, *data);
    }
  }

}
