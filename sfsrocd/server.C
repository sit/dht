/* $Id: server.C,v 1.16 2001/07/05 14:11:37 fdabek Exp $ */

/*
 *
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
 * Copyright (C) 1999, 2000 Kevin Fu (fubob@mit.edu)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include <sfsrocd.h>
#include "xdr_suio.h"
#include "rxx.h"
#include "sfsrodb_core.h"

cache_stat cstat;

#define cmtu 8192

void
server::sendreply (nfscall *sbp, void *res)
{
  sbp->reply (res);
}

/* internal routine called by *_reply below.  Just checks hash and errors */
int
server::getreply (nfscall *sbp, sfsro_datares *rores, clnt_stat err,
		  sfsro_data *data, const sfs_hash *fh)
{
  auto_xdr_delete axd (sfsro_program_1.tbl[SFSROPROC_GETDATA].xdr_res, rores);

  if (err) {
    if (err == RPC_CANTSEND || err == RPC_CANTRECV)
      getnfscall (sbp);
    else
      sbp->reject (SYSTEM_ERR);
    return 0;
  }

  // XXXXX uh, are you sure SFSRO errors map to NFS3 errors??
  if (rores->status) {
    sbp->error (nfsstat3 (rores->status) );
    return 0;
  }
  char *resbuf = rores->resok->data.base();
  size_t reslen = rores->resok->data.size();

  //  warn << "getreply: checking hash of " << hexdump (resbuf, reslen) << "\n";
  /* check hash of unmarshalled data */
  if (!sfsrocd_noverify &&
      !verify_sfsrofh (IV, SFSRO_IVSIZE, fh, resbuf, reslen)) {
    sbp->error (NFS3ERR_IO);
    return 0;
  }

  xdrmem x (resbuf, reslen, XDR_DECODE);
  bool ok = xdr_sfsro_data (x.xdrp (), data);
  if (!ok) {
    warn << "getreply: couldn't unmarshall data\n";
    sbp->error (NFS3ERR_IO);
    return 0;
  }
  return 1;
}

void
server::inode_reply (time_t rqtime, nfscall *sbp, cbinode_t cb, ref<const sfs_hash> fh,
		     sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data;

  // getreply either returns good data, or sends an NFS error
  // need to fix to return   NFS3ERR_NOENT?
  if (getreply (sbp, rores, err, &data, fh)) {
    assert (data.type == SFSRO_INODE);

    if (!sfsrocd_nocache)
      ic.enter (*fh, &(*data.inode));

    cb (&(*data.inode));
  }
}

void
server::dir_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh,
		   cbdirent_t cb, sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data;

  if (getreply (sbp, rores, err, &data, fh)) {
    assert(data.type == SFSRO_DIRBLK);

    if (!sfsrocd_nocache)
      dc.enter (*fh, &(*data.dir));

    cb (&(*data.dir));
  }
}

void
server::block_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh, 
		     cbblock_t cb, sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data;

  if (getreply (sbp, rores, err, &data, fh)) {
    assert(data.type == SFSRO_FILEBLK);

    if (!sfsrocd_nocache)
      bc.enter (*fh, &(*data.data));

    cb (data.data->base (), data.data->size ());
  }
}


void
server::indir_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh, 
		     cbindir_t cb, sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data; 

  if (getreply (sbp, rores, err, &data, fh)) {
    assert(data.type == SFSRO_INDIR);

    if (!sfsrocd_nocache)
      ibc.enter (*fh, &(*data.indir));

    cb (&(*data.indir));
  }
}
 
void
server::read_blockres (size_t b, ref<const fattr3> fa,
		       nfscall *sbp, const char *d, size_t len)
{
  read3args *ra = sbp->template getarg<read3args> ();
  read3res nfsres (NFS3_OK);

  //  size_t off = b * blocksize;
  size_t start = ra->offset % blocksize;
  warn << " client asked for " << ra->count << " and we fetched " << len << "\n";

  size_t n = MIN (MIN (ra->count, blocksize), len - start);
  
  warn << " copying " << n << "from offset " << start << "\n";

  nfsres.resok->count = n;
  nfsres.resok->eof = (fa->size >= ra->offset + n) ? 1 : 0;
  nfsres.resok->data.setsize(n);
  memcpy (nfsres.resok->data.base(), d + start, nfsres.resok->data.size ()); 
  nfsres.resok->file_attributes.set_present(1);
  *nfsres.resok->file_attributes.attributes = *fa;
  sbp->reply (&nfsres);
}

void 
server::read_block (const sfs_hash *fh, nfscall *sbp, cbblock_t cb)
{
  const rpc_bytes<RPC_INFINITY> *data;
   
  cstat.bc_tot++;
  if (!sfsrocd_nocache &&
      (data = bc.lookup (*fh))) {
    //    warn << "cached block\n";
    cb (data->base (), data->size ());
    cstat.bc_hit++;
  }
  else {
    
    cstat.bc_miss++;
    
    list<pfpstate, &pfpstate::link> *pfcbs = pfpc[*fh];
    if (pfcbs != NULL) {
      pfpstate *next = New pfpstate (cb);
      pfcbs->insert_head(next);
      return;
    }

    warn << "non pre-fectched read\n";
    ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (*fh);

    get_data(fh_ref, wrap (mkref (this), &server::block_reply,
			   timenow, sbp, fh_ref, cb));
  }
}

void
server::get_data (const sfs_hash *fh, 
		  callback<void, sfsro_datares *, clnt_stat>::ref cb) 
{
  
  sfsro_datares *res = New sfsro_datares ();
  sfsro_getarg arg;
  arg.fh = *fh;
  arg.len = cmtu;
  arg.offset = 0;

  sfsroc->call (SFSROPROC_GETDATA, &arg, res, wrap(this, &server::get_data_one_cb, cb, res, fh));

}

void
server::get_data_one_cb (callback<void, sfsro_datares *, clnt_stat>::ref cb,
			 sfsro_datares *res, const sfs_hash *fh, clnt_stat err) 
{
  
  warn << "reply status " << res->status << "\n";
  warn << res->resok->attr.size << " " << res->resok->data.size () << 
    " " << res->resok->offset << "\n";

  if ((err) || (res->status != SFSRO_OK)) {
    res->set_status(SFSRO_ERRNOENT);
    (*cb)(res, RPC_FAILED);
  } else if (res->resok->attr.size == res->resok->data.size () ) {
    (*cb)(res, RPC_SUCCESS);  
  }  else {
    warn << res->resok->data.size () << " at " << res->resok->offset << " of " << res->resok->attr.size << "\n";

    sfsro_datares *fres = New sfsro_datares ();
    fres->set_status (SFSRO_OK);
    fres->resok->data.setsize(res->resok->attr.size);
    memcpy (fres->resok->data.base (), res->resok->data.base (), 
	    res->resok->data.size ());

    unsigned int offset = res->resok->data.size ();
    unsigned int *read = New unsigned int(offset);
    sfsro_getarg arg;
    arg.fh = *fh;
    do {
      unsigned int n = (cmtu + offset < res->resok->attr.size) 
	? cmtu : res->resok->attr.size - offset;
      arg.offset = offset;
      arg.len = n;
      sfsro_datares *pres = New sfsro_datares();
      warn << "getting " << arg.len << " at " << arg.offset << "\n";
      sfsroc->call (SFSROPROC_GETDATA, &arg, res,
		    wrap(this, &server::get_data_cb, cb, pres, fres, read));
      offset += n;
    } while (offset < res->resok->attr.size);
    delete res;
  }

}

void
server::get_data_cb (callback<void, sfsro_datares *, clnt_stat>::ref cb,
		     sfsro_datares *res, sfsro_datares *fres, 
		     unsigned int *read, clnt_stat err) 
{
  if ((err) || (res->status != SFSRO_OK))
    cb(res, RPC_FAILED);
  else {
    warn << res->resok->data.size () << " at " << res->resok->offset << "\n";
    memcpy((char *)fres->resok->data.base () + res->resok->offset,
	   res->resok->data.base (),
	   res->resok->data.size ());
    *read += res->resok->data.size ();
    
    if (*read == res->resok->attr.size) {
      delete read;
      cb (fres, RPC_SUCCESS);
    }
  }
  delete res;
}

void 
server::read_indir (const sfs_hash *fh, nfscall *sbp, cbindir_t cb)
{

  const sfsro_indirect *indir;

  cstat.ibc_tot++;
  if (!sfsrocd_nocache &&
      (indir = ibc.lookup (*fh))) {
    //    warn << "cached indirect\n";
    cb (indir);
    cstat.ibc_hit++;
  }
  else {
    //    warn << "noncache indirect\n";
    cstat.ibc_miss++;

    warn << "reading indirect page from " << hexdump(fh, 20) << "\n";

    ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (*fh);
    get_data (fh, wrap (mkref (this), &server::indir_reply,
			timenow, sbp, fh_ref, cb));
  }
}

void
server::read_indirectres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
			  const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR);

  /* the following could happen only by a call from
     read_doubleres or read_tripleres */
  i = i % nfh;

  warn << " reading " << i << "th indirect block\n";

  ref<const sfs_hash> block_fh =
    New refcounted<const sfs_hash> (indirect->handles[i]);
  
  warn << "block_fh is " << hexdump(block_fh, 20) << "\n";

  read_block(block_fh, sbp,
	     wrap (mkref (this), &server::read_blockres, b, fa, sbp));
}

void
server::read_doubleres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
			const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR) - nfh;

  /* the following could happen only by a call from read_tripleres */
  i = i % (nfh * nfh);

  i = i / nfh;

  read_indir(&indirect->handles[i], sbp,
	     wrap (mkref (this), &server::read_indirectres, b, fa, sbp));
}


void
server::read_tripleres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
			const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR) - nfh - (nfh * nfh);

  i = i / (nfh * nfh);

  read_indir (&indirect->handles[i], sbp,
	     wrap (mkref (this), &server::read_doubleres, b, fa, sbp));
}


/* b is the block number */
void
server::fh_lookup (size_t b, const sfsro_inode *ip, nfscall *sbp,
		   const sfs_hash *fh)
{
  fattr3 fa;
  ro2nfsattr(ip, &fa, fh);
  ref<const fattr3> fa_ref (New refcounted<const fattr3> (fa));

  warn << "asking for block " << b << "/" << SFSRO_NDIR << "\n";

  if (b < SFSRO_NDIR) {
    read_block(&(ip->reg->direct[b]), sbp,
	       wrap ( mkref (this),
		      &server::read_blockres, b, fa_ref, sbp));
  }
  else {
     size_t i = (b - SFSRO_NDIR);

     if (i < nfh) {
       warn << "reading indirect page from " << hexdump(&(ip->reg->indirect), 20) << "\n";
       read_indir(&(ip->reg->indirect), sbp,
		  wrap ( mkref (this),
			 &server::read_indirectres, b, fa_ref, sbp));
     }
     else {
       i -= nfh;

       if (i < nfh * nfh)
	 read_indir(&(ip->reg->double_indirect), sbp,
	       wrap ( mkref (this),
		      &server::read_doubleres, b, fa_ref, sbp));
       else {
	 i -= nfh * nfh;
	 
	 if (i < nfh * nfh * nfh)
	   read_indir(&(ip->reg->triple_indirect), sbp,
		      wrap ( mkref (this), 
			     &server::read_tripleres, b, fa_ref, sbp));
	 else
	   assert(0);  // too big
       }
     }
  }
}


void 
server::read_file (const sfsro_inode *ip, nfscall *sbp, 
		   const sfs_hash *fh)
{
  read3args *ra = sbp->template getarg<read3args> ();

  if (ra->count < 1)
    return;

  warn << "read of " << ra->count << " at " << ra->offset << "\n";

  size_t block = ra->offset / blocksize;
  //  size_t last_blknr = (ra->offset + (ra->count - 1) ) / blocksize;
  size_t prefetch_limit = block + sfsrocd_prefetch;

  fh_lookup (block, ip, sbp, fh);

  if (sfsrocd_prefetch)
    for (size_t i = block + 1; i <= prefetch_limit; i++) 
      if ((i+1)*blocksize < ip->reg->size) 
	fh_prefetch(i, ip, fh);


}

void 
server::readinode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			     const sfsro_inode *ip)
{
  read3args *ra = sbp->template getarg<read3args> ();

  if (!(ip->type == SFSROREG
	|| ip->type == SFSROREG_EXEC 
	|| ip->type == SFSROLNK))
    sbp->error (NFS3ERR_IO);
  else if (ra->offset >= ip->reg->size) {
    read3res nfsres(NFS3_OK);
    fattr3 fa;
    
    ro2nfsattr(ip, &fa, fh);
    nfsres.resok->count = 0;
    nfsres.resok->eof = 1;
    nfsres.resok->file_attributes.set_present(1);
    *nfsres.resok->file_attributes.attributes = fa;
    sbp->reply(&nfsres);
  } else {
    read_file(ip, sbp, fh);
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

BOOL
readdir_xdr (XDR *x, void *_uio)
{
  assert (x->x_op == XDR_ENCODE);

  suio *uio = static_cast<suio *> (_uio);
   
  xsuio (x)->take (uio);

  return true;

}

void
server::readdir_lookupres (nfscall *sbp, const sfsro_directory *dir)
{
  readdir3args *nfsargs = sbp->template getarg<readdir3args> ();
  readdir3res *nfsres = sbp->template getres<readdir3res> ();
  bool errors = false;

  xdrsuio x (XDR_ENCODE, true);

  if (!xdr_putint (&x, NFS_OK))
    errors = true;

  if (!xdr_post_op_attr (&x, &nfsres->resok->dir_attributes) 
      || !xdr_puthyper (&x, 0))
    errors = true;

  sfsro_dirent *roe = dir->entries;

  // XXX linear kludge to begin at offset.  Fix with btree

  uint32 i = 0;
  while (roe && nfsargs->cookie > i) {
    roe = roe->nextentry;
    i++;
  }
  
  uint64 fileid;
  
  for (uint32 j = 0; 
       roe && (xdr_getpos (&x) + 24 + roe->name.len () <= nfsargs->count);
       j++) {
    
    server::fh2fileid (&roe->fh, &fileid);

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
server::readdirplus_lookupres (nfscall *sbp, readdirplus3res *nfsres,
			       const sfsro_directory *dir)
{
  readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();

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
    //    * ((*last)->name_handle.handle) = * (nfs_fh3 *) &(roe->fh);
    last = &(*last)->nextentry;
    roe = roe->nextentry;
  }
  nfsres->resok->reply.eof = (i < n) ? 0 : 1;
  sendreply(sbp, nfsres);
}

/*
  Take foo/bar/baz -> vec of v[0]=baz, v[1]=bar, v[2]=foo
  (backwards)
*/
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


void 
server::dir_lookup_parentres (nfscall *sbp, ref<const sfs_hash> dir_fh,
			      const sfs_hash *fh)
{
  ref<const sfs_hash> obj_fh 
    (New refcounted<const sfs_hash> (*fh));
  
  inode_lookup (dir_fh, sbp, 
		wrap (this, &server::dir_lookupres_dir_attr, 
		      sbp, dir_fh, obj_fh));
}


void
server::dir_lookupres (nfscall *sbp, const sfsro_directory *dir)
{
  diropargs3 *dirop = sbp->template getarg<diropargs3> ();

  sfs_hash fh;
  nfs2ro(&dirop->dir, &fh);
  ref<const sfs_hash> dir_fh = New refcounted<const sfs_hash> (fh);

  if (dirop->name == "."  
      || (dirop->name == ".." && dir->path.len () == 0))
    {
      inode_lookup (&fh, sbp, 
		    wrap (this, &server::dir_lookupres_dir_attr, 
			  sbp, dir_fh, dir_fh));
    }
  else if (dirop->name == "..")
    {
      warn << "Looking up a dot dot w.r.t. " << dir->path << "\n";

      vec<str> pv;
      splitpath (pv, dir->path);
      pv.pop_back ();
      ref<vec<str> > pathvec (New refcounted<vec<str> > (pv));

      lookup_parent_rofh (sbp, &fsinfo->sfsro->v1->info.rootfh, 
			  pathvec,
			  wrap (this, &server::dir_lookup_parentres,
				sbp, dir_fh));
    } else {
      
      const sfsro_dirent *dirent = dirent_lookup(sbp, &(dirop->name), dir);
      
      if (dirent == NULL) {
	///// XXX do we need to send post_op_attr if errors?
	warn << "returning NOENT in dir_lookupres\n";
	sbp->error (NFS3ERR_NOENT);
      } else {
	ref<const sfs_hash> obj_fh 
	  (New refcounted<const sfs_hash> (dirent->fh));
	
	inode_lookup (&fh, sbp, 
		      wrap (this, &server::dir_lookupres_dir_attr, 
			    sbp, dir_fh, obj_fh));
		      
      }
    }
}


void
server::dir_lookupres_dir_attr (nfscall *sbp, 
				ref<const sfs_hash> dir_fh,
				ref<const sfs_hash> obj_fh,
				const sfsro_inode *dir_ip)
{
  lookup3res *nfsres = sbp->template getres<lookup3res> ();
  nfsres->set_status (NFS3_OK);

  nfsres->resok->dir_attributes.set_present (true);

  fattr3 fa;
  ro2nfsattr (dir_ip, &fa, dir_fh);
  *nfsres->resok->dir_attributes.attributes = fa;

  inode_lookup (obj_fh, sbp, wrap (this, &server::dir_lookupres_obj_attr, 
				   sbp, obj_fh, nfsres));
}


void
server::dir_lookupres_obj_attr (nfscall *sbp, 
				ref<const sfs_hash> obj_fh,
				lookup3res *nfsres,
				const sfsro_inode *obj_ip)
{
 
  nfs_fh3 nfh;
  ro2nfs(obj_fh, &nfh);

  nfsres->resok->object = nfh;
  nfsres->resok->obj_attributes.set_present (true);

  fattr3 fa;
  ro2nfsattr (obj_ip, &fa, obj_fh);
  *nfsres->resok->obj_attributes.attributes = fa;

#if 0
  if (obj_ip->type == SFSROREG) {
    warn << "will prefetch from LOOKUP\n";
    fh_prefetch (0, obj_ip, obj_fh);
  }
#endif

  sendreply(sbp, nfsres);
}

void
server::dir_lookup (const sfsro_inode *ip, nfscall *sbp, cbdirent_t cb)
{
  const sfsro_directory *dir;
  
  cstat.dc_tot++;
  if (!sfsrocd_nocache && (dir = dc.lookup (ip->reg->direct[0]))) {
    //    warn << "cached lookup\n";
    cb (dir);
    cstat.dc_hit++;
  } else {
    //        warn << "noncache lookup\n";
    cstat.dc_miss++;
    //    sfsro_datares *res = New sfsro_datares (SFSRO_OK);
    
    ref<const sfs_hash> fh_ref = 
      New refcounted<const sfs_hash> (ip->reg->direct[0]);

    get_data(&ip->reg->direct[0], wrap (mkref (this), &server::dir_reply,
					timenow, sbp, fh_ref, cb));
  }
}

void
server::lookupinode_lookupres (nfscall *sbp, const sfsro_inode *ip)
{
  
  //  warn << ip->reg->size << " " << ip->reg->direct.size() << "\n";
  if (ip->type != SFSRODIR && ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else if (ip->reg->direct.size() <= 0) {
    warn << "returning NOENT in lookupinode+lookupres\n";
    sbp->error (NFS3ERR_NOENT);
  }  else
    dir_lookup(ip, sbp, wrap (this, &server::dir_lookupres, sbp));
}

void
server::readdirinode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			       const sfsro_inode *ip)
{

  if (ip->type != SFSRODIR && ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else if (ip->reg->direct.size() <= 0) {
    warn << "returning NOENT in readdirinode+lookupres\n";
    sbp->error (NFS3ERR_NOENT);
  } else {

    readdir3res *nfsres = sbp->template getres<readdir3res> ();
    rpc_clear (*nfsres);
    nfsres->set_status (NFS3_OK);
    nfsres->resok->dir_attributes.set_present (true);

    fattr3 fa;
    ro2nfsattr (ip, &fa, fh);
    *nfsres->resok->dir_attributes.attributes = fa;

    dir_lookup (ip, sbp, wrap (this, &server::readdir_lookupres, sbp));
      
  }
}

void
server::readdirplusinode_lookupres(nfscall *sbp, 
				   ref<const sfs_hash> fh,
				   const sfsro_inode *ip)
{
  if (ip->type != SFSRODIR && ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else if (ip->reg->direct.size() <= 0) {
    warn << "returning NOENT in readdirplusinode+lookupres\n";
    sbp->error (NFS3ERR_NOENT);
}  else {
    readdirplus3res *nfsres = sbp->template getres<readdirplus3res> ();
    rpc_clear (*nfsres);
    nfsres->set_status (NFS3_OK);
    nfsres->resok->dir_attributes.set_present (true);

    fattr3 fa;
    ro2nfsattr (ip, &fa, fh);
    *nfsres->resok->dir_attributes.attributes = fa;

    dir_lookup (ip, sbp, 
		wrap (this, &server::readdirplus_lookupres, sbp, nfsres));
  }
}

void
server::accessinode_lookupres(nfscall *sbp, const sfsro_inode *ip)
{
  access3args *aa = sbp->template getarg<access3args> ();
  uint32 access_req = aa->access;
  access3res nfsres (NFS3_OK);

  // XXXXX if ip == NULL???

  nfsres.resok->access = access_check (ip, access_req);

  sbp->reply (&nfsres);
  
}

void
server::readlinkinode_lookupres (nfscall *sbp, const sfsro_inode *ip)
{
  if (ip->type != SFSROLNK) 
    sbp->error (NFS3ERR_INVAL);
  else if (ip->lnk->dest.len () <= 0)
    sbp->error (NFS3ERR_INVAL);   // ?????
  else if (ip->lnk->dest.len () >= blocksize)
    sbp->error (NFS3ERR_NOTSUPP);   // XXX
  else {
    readlink3res nfsres (NFS3_OK);

    nfsres.resok->data = ip->lnk->dest;
    sbp->reply(&nfsres);
    
  }
}

void
server::inode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			 const sfsro_inode *ip)
{
  getattr3res nfsres (NFS3_OK);
  fattr3 fa;

  ro2nfsattr (ip, &fa, fh);

  //XX hack to test .. and timeout attributes

  /*  fa.mtime.seconds = static_cast<uint32>(timenow);
  fa.mtime.nseconds = 0;
  */

  *nfsres.attributes = fa;
  sbp->reply (&nfsres); 
}

void
server::inode_lookup (const sfs_hash *fh, nfscall *sbp, cbinode_t cb)
{
  const sfsro_inode *inode;

  // XX cache
  cstat.ic_tot++;

  if (!sfsrocd_nocache && (inode = ic.lookup (*fh))) {
    cb (inode);

    //    warn << inode->reg->size << " " << inode->reg->direct.size() << "\n";
    // XX cache
    cstat.ic_hit++;
  } else {
    // XX cache
    cstat.ic_miss++;

    ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (*fh);
    get_data (fh, wrap (this, &server::inode_reply, timenow, sbp, cb, fh_ref));
    //    sfsroc->call (SFSROPROC_GETDATA, fh, res, 
    //		  wrap (mkref (this), &server::inode_reply, 
    //			timenow, sbp, cb, res, fh_ref));
  }
}

void
server::nfs3_fsstat (nfscall *sbp)
{
  fsstat3res res (NFS3_OK);
  rpc_clear (res);
  sbp->reply (&res);
}

void
server::nfs3_fsinfo (nfscall *sbp)
{
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

void
server::dispatch (nfscall *sbp)
{
  //  warn << "dispatch: " << sbp->proc() << "\n";
  switch(sbp->proc()) {
  case NFSPROC3_NULL:
    sbp->reply (NULL);
    break;
  case NFSPROC3_GETATTR: 
    {
      nfs_fh3 *nfh = sbp->template getarg<nfs_fh3> ();
      sfs_hash fh;
      nfs2ro(nfh, &fh);
      ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (fh);

      inode_lookup (&fh, sbp, wrap (this, &server::inode_lookupres, sbp, 
				    fh_ref));
    }
    break;
  case NFSPROC3_LOOKUP: 
    {
      diropargs3 *dirop = sbp->template getarg<diropargs3> ();
      nfs_fh3 *nfh = &(dirop->dir);
      
      if (memcmp(nfh->data.base (), rootfh.data.base(), nfh->data.size()) == 0)
	{
	  //case I: lookup is on root level and must be translated into a 
	  //        request for fsinfo.
	  
	  //  warn << "mount request for " << dirop->name.cstr() << " (" << dirop->name.len () << ")\n";
	  sfs_hash *fsinfo_fh = New sfs_hash ();
	  memcpy(fsinfo_fh->base(), dirop->name.cstr(), 20);
	  
	  sfsro_datares *res = (sfsro_datares *)rfhc.lookup(*fsinfo_fh);
	  if (res) {
	    //warn << "HIT on rfhc\n";
	    lookup_mount(sbp, fsinfo_fh, res, clnt_stat(0));
	  } else {
	    get_data(fsinfo_fh, wrap(this, &server::lookup_mount, sbp, fsinfo_fh));
	    warn << "MISS on rfhc\n";
	  }
	} 
      else
	{

	  // case II: lookup not on root level. "Normal" case.
	  sfs_hash fh;
	  nfs2ro(nfh, &fh);
	  
	  inode_lookup (&fh, sbp, 
			wrap (this, &server::lookupinode_lookupres, sbp));
	}
    }
    break;
  case NFSPROC3_ACCESS:
    {
      access3args *aa = sbp->template getarg<access3args> ();
      nfs_fh3 *nfh = &(aa->object);

      sfs_hash fh;
      nfs2ro(nfh, &fh);

      inode_lookup (&fh, sbp, 
		    wrap (this, &server::accessinode_lookupres, sbp));
    }
    break;
  case NFSPROC3_READLINK:
    {
      nfs_fh3 *nfh =  sbp->template getarg<nfs_fh3> ();

      sfs_hash fh;
      nfs2ro(nfh, &fh);

      inode_lookup (&fh, sbp, 
		    wrap (this, &server::readlinkinode_lookupres, sbp));
    }
    break;
  case NFSPROC3_READ:
    {
      read3args *ra = sbp->template getarg<read3args> ();
      nfs_fh3 *nfh = &(ra->file);

      warn << "read of " << ra->count << " at " << ra->offset << "\n";
      sfs_hash fh;
      nfs2ro(nfh, &fh);
      ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (fh);

      inode_lookup (&fh, sbp, wrap (this, &server::readinode_lookupres,
				    sbp, fh_ref));
    }
    break;
  case NFSPROC3_READDIR:
    {
      readdir3args *readdirop = sbp->template getarg<readdir3args> ();
      
      nfs_fh3 *nfh = &(readdirop->dir);
      
      sfs_hash fh;
      nfs2ro(nfh, &fh);
      ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (fh);
      
      inode_lookup (&fh, sbp, 
		    wrap (this, &server::readdirinode_lookupres,
			  sbp, fh_ref));
      
    }
    break;
  case NFSPROC3_READDIRPLUS:
    {
      readdirplus3args *readdirop = sbp->template getarg<readdirplus3args> ();

      nfs_fh3 *nfh = &(readdirop->dir);

      sfs_hash fh;
      nfs2ro(nfh, &fh);
      ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (fh);
      
      inode_lookup (&fh, sbp, 
		    wrap (this, &server::readdirplusinode_lookupres,
			  sbp, fh_ref));
    }
    break;
  case NFSPROC3_FSSTAT:
    nfs3_fsstat (sbp);
    break;
  case NFSPROC3_FSINFO:
    nfs3_fsinfo (sbp);
    break;
  default:
    sbp->error (NFS3ERR_ROFS);
    break;
  }
}

void
server::lookup_mount(nfscall *sbp, sfs_hash *fh, sfsro_datares *res, clnt_stat err) {

  if (err) {
    warn << "error looking up mount point\n";
    sbp->error (NFS3ERR_NOENT);
    return;
  }
  
  rfhc.enter (*fh, res);

  sfs_fsinfo *mounted_fsinfo = New sfs_fsinfo();
  xdrmem x (static_cast<char *>(res->resok->data.base ()), res->resok->data.size (), XDR_DECODE);
  if (!xdr_sfs_fsinfo (x.xdrp (), mounted_fsinfo)) {
    warn << "error decoding returned fsinfo\n";
    sbp->error (NFS3ERR_NOENT);
    return;
  }
  
  blocksize = mounted_fsinfo->sfsro->v1->info.blocksize;
  nfh = blocksize / (20*2);
  if (nfh > SFSRO_NFH) nfh = SFSRO_NFH;

  warn << "block size is " << blocksize << "\n";
  warn << "nfh is " << nfh << "\n";

  //  warn << "mounted root fh=" << hexdump (&mounted_fsinfo->sfsro->v1->info.rootfh, 20) << "\n";
  
  ref<const sfs_hash> root_fh = New refcounted<const sfs_hash> (fsinfo->sfsro->v1->info.rootfh);
  ref<const sfs_hash> obj_fh 
    (New refcounted<const sfs_hash> (mounted_fsinfo->sfsro->v1->info.rootfh));
	
  inode_lookup(&fsinfo->sfsro->v1->info.rootfh, sbp, 
	       wrap (this, &server::dir_lookupres_dir_attr, 
		     sbp, root_fh, obj_fh));

}

bool
server::setrootfh (const sfs_fsinfo *fsi)
{



  warn << "root fh is " << hexdump(&fsi->sfsro->v1->info.rootfh, 20) << "\n";

  //  warn << "prog= " << fsi->prog << " isn't " << SFSRO_PROGRAM << " or vers= " << fsi->sfsro->vers << " isn't " << SFSRO_VERSION << "\n";
  if (fsi->prog != SFSRO_PROGRAM || fsi->sfsro->vers != SFSRO_VERSION) 
    return false;
  
  if (!sfsrocd_noverify
      && !verify_sfsrosig (&fsi->sfsro->v1->sig, &fsi->sfsro->v1->info,
			   &hinfo.pubkey)) {
    warn << "failed to verify signature " << path << "\n";
    return false;
  }

  if (!fsinfo)
    memcpy (IV, fsi->sfsro->v1->info.iv.base (), SFSRO_IVSIZE);
  else if (memcmp (IV, fsi->sfsro->v1->info.iv.base (), SFSRO_IVSIZE)) {
    warn << "IV changed " << path << "\n";
    return false;
  }

  sfsroc = aclnt::alloc (x, sfsro_program_1);
  ro2nfs (&fsi->sfsro->v1->info.rootfh, &rootfh);


  return true;
}

/* Assumes name is not . or .. */
const sfsro_dirent *
server::dirent_lookup (nfscall *sbp, filename3 *name, 
		       const sfsro_directory *dir)
{
  assert (*name != ".");
  assert (*name != "..");
  
  /*
    strbuf sb;
    strbuf sb1;
    
    
    rpc_print (sb, *name, 5, NULL, " ");
    rpc_print (sb1, *dir, 50, NULL, " ");
  */
  
  // warn << "dirent_lookup: lookup " << sb << " in " << sb1 << "\n";

  /* XXX should make this a binary search to prove that non-existent
     files do not exist */


  sfsro_dirent *e = NULL, *e_prev = NULL;

  //  warn << "dirent_lookup: name is " << *name << "\n";

  for (e = e_prev = dir->entries; e; e = e->nextentry) {
     if (*name == e->name)
       {
	 //	 fh2fileid (&e->fh, &e->fileid);
	 return e;
       }
     if ((e_prev->name < *name) &&
	 (*name < e->name))
       {
	 //	 warn << "dirent_lookup: secure no match\n";
	 return NULL;
       }

     e_prev = e;
  }

  //  warn << "dirent_lookup: secure no match\n";
  return NULL;
}


void 
server::ro2nfsattr (const sfsro_inode *si, fattr3 *ni, const sfs_hash *fh)
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
    //    warn << "server::ro2nfsattr:"  << hexdump (fh, 20) << "\n";
    fatal ("server::ro2nfsattr: Unknown si->type %X\n",
	   si->type);
    break;
  }
    
  ni->uid = sfs_uid;
  ni->gid = sfs_gid;
  ni->rdev.minor = 0;
  ni->rdev.major = 0;
  ni->fsid = 0;
  fh2fileid (fh, &ni->fileid);
    
  //ni->mtime.seconds = ni->ctime.seconds = static_cast<uint32>(timenow);
  //ni->mtime.nseconds = ni->ctime.nseconds = 0;
  // To fool the attribute cache
 
}


uint32
server::access_check(const sfsro_inode *ip, uint32 access_req)
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
server::ro2nfs (const sfs_hash *fh, nfs_fh3 *nfh)
{
  nfh->data.setsize (fh->size ());
  memcpy(nfh->data.base(), fh->base (), fh->size ());
}

bool
server::nfs2ro (nfs_fh3 *nfh, sfs_hash *fh)
{
  if (nfh->data.size () != fh->size ()) {
    bzero (fh->base (), fh->size ());
    return false;
  }
  memcpy (fh->base (), nfh->data.base(), fh->size ());
  return true;
}


// XXXXXXXX the 64 to 32 bit conversion is gross
void
server::fh2fileid (const sfs_hash *fh, uint64 *fileid)
{
  *fileid = gethyper (fh->base ());
}
 



/* Given: File handle (dir_fh) of a directory, a path relative to
   this directory, a callback to return a file handle

   Effects: If path is a file name (no '/' in name), call cb(fh) 
   where fh is file handle of the path w.r.t. the dir_fh.

   Otherwise, recursively make a callback to lookup the next
   subdirectory.

*/
void
server::lookup_parent_rofh (nfscall *sbp, const sfs_hash *dir_fh, 
			    ref<vec<str> > pathvec, cbparent_t cb)  
{

  if (pathvec->empty ()) {
    cb (dir_fh);
  } else {
    inode_lookup (dir_fh, sbp, wrap (this, 
				     &server::lookup_parent_rofh_lookupres,
				     sbp, pathvec, cb));
  }

}



void
server::lookup_parent_rofh_lookupres (nfscall *sbp, 
				ref<vec <str> > pathvec,
				cbparent_t cb,
				const sfsro_inode *ip)
{

  if (ip->type != SFSRODIR && ip->type != SFSRODIR_OPAQ) 
    sbp->error (NFS3ERR_NOTDIR);
  else if (ip->reg->direct.size() <= 0) {
    warn << "returning NOENT in lookup_parent_rofh_lookupres\n";
    sbp->error (NFS3ERR_NOENT);
}  else
    dir_lookup (ip, sbp, 
		wrap (this, &server::lookup_parent_rofh_lookupres2,
		      sbp, pathvec, cb));


}

void
server::lookup_parent_rofh_lookupres2 (nfscall *sbp,
				       ref<vec <str> > pathvec,
				       cbparent_t cb,
				       const sfsro_directory *dir)
{
  filename3 filename (pathvec->pop_front ());
  
  const sfsro_dirent *dirent = dirent_lookup(sbp, &filename, dir);

  if (dirent == NULL) {
    warn << "returning NOENT in lookup_parent_rofh_lookupres2\n";
    sbp->error (NFS3ERR_NOENT);
} else
    lookup_parent_rofh (sbp, &dirent->fh, pathvec, cb);
}

//------------------------ prefetch -----------------------------------

/* b is the block number */
// fh is the inode "number"
void
server::fh_prefetch (size_t b, const sfsro_inode *ip, 
		     const sfs_hash *fh)
{

  if (b < SFSRO_NDIR) {
    prefetch_block(&(ip->reg->direct[b]),
		   wrap ( mkref (this),
			  &server::prefetch_blockres, fh, b));
  }

  else {
    size_t i = (b - SFSRO_NDIR);
    
    if (i < nfh) {
      prefetch_indir(&(ip->reg->indirect),
		     wrap ( mkref (this),
			    &server::prefetch_indirectres, b));
    }
    else {
      i -= nfh;
      
      if (i < nfh * nfh)
	prefetch_indir(&(ip->reg->double_indirect),
	       wrap ( mkref (this),
		      &server::prefetch_doubleres, b));
      else {
	i -= nfh * nfh;
	
	if (i < nfh * nfh * nfh)
	  prefetch_indir(&(ip->reg->triple_indirect),
		     wrap ( mkref (this), 
			    &server::prefetch_tripleres, b));
	else
	  assert(0);  // too big
      }
    }
  }

}


void
server::prefetch_blockres (const sfs_hash *fh, size_t b, const char *d, size_t len)
{
  //save b, d, len under fh (which is the hash of the inode)
  //data is in the cache waiting for the request

  warn << "fetched and cached " << hexdump(fh, 20) << "\n";
}


void 
server::prefetch_block (const sfs_hash *fh, cbblock_t cb)
{
  const rpc_bytes<RPC_INFINITY> *data;
  
  if (pfpc[*fh] != NULL) {
    warn << hexdump(fh, 20) << "already in flight\n";
    return;
  }
  warn << "prefetch block " << hexdump (fh, 20) << "\n";
  cstat.bc_tot++;
  if (!sfsrocd_nocache &&
      (data = bc.lookup (*fh))) {
    //    warn << "cached block\n";
    cb (data->base (), data->size ());
    cstat.bc_hit++;
  }
  else {
    cstat.bc_miss++;
    
    ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (*fh);
    
    pfpstate *head = New pfpstate (cb);
    list<pfpstate, &pfpstate::link> cblist;
    cblist.insert_head (head);
    pfpc.insert (*fh, cblist);
    
    get_data(fh_ref, wrap (mkref (this), &server::prefetch_block_reply,
			   timenow, fh_ref));
  }
}

void
server::prefetch_block_reply (time_t rqtime, ref<const sfs_hash> fh, 
			      sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data;

  if (pfreply(rores, err, &data, fh)) {
    assert(data.type == SFSRO_FILEBLK);
    
    if (!sfsrocd_nocache)
      bc.enter (*fh, &(*data.data));

    list<pfpstate, &pfpstate::link> *cb_list = pfpc[*fh];
    pfpstate *p = cb_list->first;
    while (p) {
      (p->cb) (data.data->base (), data.data->size ());
      pfpstate *next = cb_list->next (p);
      cb_list->remove (p);
      p = next;
    }
    pfpc.remove(*fh);

  }
}


void 
server::prefetch_indir (const sfs_hash *fh, cbindir_t cb)
{

  const sfsro_indirect *indir;

  cstat.ibc_tot++;
  if (!sfsrocd_nocache &&
      (indir = ibc.lookup (*fh))) {
    cb (indir);
    cstat.ibc_hit++;
  }
  else {

    cstat.ibc_miss++;

    ref<const sfs_hash> fh_ref = New refcounted<const sfs_hash> (*fh);
    get_data (fh, wrap (mkref (this), &server::prefetch_indir_reply,
			timenow, fh_ref, cb));
  }
}


void
server::prefetch_indir_reply (time_t rqtime, ref<const sfs_hash> fh, 
			      cbindir_t cb, sfsro_datares *rores, clnt_stat err) 
{
  sfsro_data data; 

  if (pfreply (rores, err, &data, fh)) {
    assert(data.type == SFSRO_INDIR);
    
    if (!sfsrocd_nocache)
      ibc.enter (*fh, &(*data.indir));
    
    cb (&(*data.indir));
    
  }
}

void
server::prefetch_indirectres (size_t b, const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR);

  /* the following could happen only by a call from
     read_doubleres or read_tripleres */
  i = i % nfh;

  ref<const sfs_hash> block_fh =
    New refcounted<const sfs_hash> (indirect->handles[i]);
  
  prefetch_block(block_fh, wrap ( mkref (this),
				  &server::prefetch_blockres, block_fh, b));

}

void
server::prefetch_doubleres (size_t b, const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR) - nfh;

  /* the following could happen only by a call from read_tripleres */
  i = i % (nfh * nfh);
  i = i / nfh;
  
  prefetch_indir(&indirect->handles[i],
		 wrap (mkref (this), &server::prefetch_indirectres, b));
}


void
server::prefetch_tripleres (size_t b, const sfsro_indirect *indirect)
{
  size_t i = (b - SFSRO_NDIR) - nfh - (nfh * nfh);

  i = i / (nfh * nfh);

  prefetch_indir (&indirect->handles[i],
		  wrap (mkref (this), &server::prefetch_doubleres, b));
}

/* hacked version of getreply */
int
server::pfreply (sfsro_datares *rores, clnt_stat err,
		 sfsro_data *data, const sfs_hash *fh)
{
  auto_xdr_delete axd (sfsro_program_1.tbl[SFSROPROC_GETDATA].xdr_res, rores);

  if ((err) || (rores->status))
    return 0;

  char *resbuf = rores->resok->data.base();
  size_t reslen = rores->resok->data.size();

  /* check hash of unmarshalled data */
  if (!sfsrocd_noverify &&
      !verify_sfsrofh (IV, SFSRO_IVSIZE, fh, resbuf, reslen)) 
    return 0;

  xdrmem x (resbuf, reslen, XDR_DECODE);
  bool ok = xdr_sfsro_data (x.xdrp (), data);
  if (!ok) {
    warn << "pfreply: couldn't unmarshall data\n";
    return 0;
  }
  return 1;
}


