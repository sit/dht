// -*-c++-*-
/* $Id: sfsrocd.h,v 1.1 2001/01/16 22:00:08 fdabek Exp $ */

/*
 *
 * Copyright (C) 1998, 2000 David Mazieres (dm@uun.org)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "arpc.h"
#include <sfsro_prot.h>
#include "nfs3_prot.h"
#include "sfscd_prot.h"
#include "sfsmisc.h"
#include "qhash.h"
#include "itree.h"
#include "crypt.h"
#include "list.h"
#include <sfsclient.h>

extern blowfish fhkey;

struct cache_stat {
  u_int32_t ic_hit;
  u_int32_t ic_miss;
  u_int32_t ic_tot;

  u_int32_t dc_hit;
  u_int32_t dc_miss;
  u_int32_t dc_tot;

  u_int32_t ibc_hit;
  u_int32_t ibc_miss;
  u_int32_t ibc_tot;

  u_int32_t bc_hit;
  u_int32_t bc_miss;
  u_int32_t bc_tot;
};

extern cache_stat cstat;

#ifdef MAINTAINER
extern const bool sfsrocd_noverify;
extern const bool sfsrocd_nocache;
extern const bool sfsrocd_cache_stat;
#else /* !MAINTAINER */
enum { sfsrocd_noverify = 0, sfsrocd_nocache = 0, sfsrocd_cache_stat = 0 };
#endif /* !MAINTAINER */


template<class KEY, class VALUE, u_int max_cache_entries>
class cache {
  struct cache_entry {
    cache *const c;
    const KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;
    // should add doubly linked to quickly delete


    cache_entry (cache<KEY, VALUE, max_cache_entries> *cc,
	       const KEY &kk, const VALUE *vv)
      : c (cc), k (kk)
    {
      v = *vv;
      c->lrulist.insert_tail (this);
      c->entries.insert (this);
      c->num_cache_entries++;
      while (c->num_cache_entries > implicit_cast<u_int> (max_cache_entries))
	delete c->lrulist.first;
    }

    ~cache_entry ()
    {
      c->lrulist.remove (this);
      c->entries.remove (this);
      c->num_cache_entries--;
    }

    void touch ()
    {
      c->lrulist.remove (this);
      c->lrulist.insert_tail (this);
    }

  };
  

private:
  friend class cache_entry;
  ihash<const KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;

public:
  cache () { num_cache_entries = 0; }
  ~cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  void enter (const KEY& kk, const VALUE *vv)
  {
    cache_entry *ad = entries[kk];
    if (!ad)
      vNew cache_entry (this, kk, vv);
    else 
      ad->touch ();
  }

  const VALUE *lookup (const KEY& kk)
  {
    cache_entry *ad = entries[kk];
    if (ad) {
      ad->touch ();
      return &ad->v;
    }
    return NULL;
  }

};

struct mirror_info {
  int aclnt_index;
  int slice_start;
  int slice_len;
  long start_sec;
  long start_usec;
  long total_bytes;
  float total_ticks;
  float performance;
  long total_performance;
};

struct server : sfsserver {
  typedef callback<void, const sfsro_inode * >::ref cbinode_t;
  typedef callback<void, const sfsro_directory *>::ref cbdirent_t;
  typedef callback<void, const sfsro_indirect *>::ref cbindir_t;
  typedef callback<void, const char *, size_t>::ref cbblock_t;
  typedef callback<void, const nfspath3 *>::ref cblnk_t;
  typedef callback<void, const sfs_hash *>::ref cbparent_t;
private:
  cache<sfs_hash, sfsro_inode, 512> ic;             // inode cache
  cache<sfs_hash, sfsro_indirect, 512> ibc;         // Indirect block cache
  cache<sfs_hash, sfsro_directory, 512> dc;         // Directory block cache

  //the aclnts themselves
  vec<ptr<aclnt> > mirrors;

  //all the mirrors w/ span info (mirror in)
  mirror_info mi[64];
  //the spans we should fetch from each mirror to get the whole block (mirror out)
  mirror_info mo[64];
  int mo_size;

  // we need to reduce the miss penalty here.  required if
  // we want "compression"
  cache<sfs_hash, rpc_bytes<RPC_INFINITY>, 64> bc;  // file data buffer cache
  equals<sfs_hash> eq;

  void ro2nfsattr (const sfsro_inode *si, fattr3 *ni, const sfs_hash *fh);

  const sfsro_dirent *dirent_lookup(nfscall *sbp, filename3 *name, 
				    const sfsro_directory *dir);
  uint32 access_check(const sfsro_inode *ip, uint32 access_req);

  void inode_reply (time_t rqtime, nfscall *sbp, cbinode_t cb, 
		    sfsro_datares *rores, ref<const sfs_hash> fh,
		    clnt_stat err);
  void dir_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh, 
		  cbdirent_t cb, sfsro_datares *rores, clnt_stat err);
  void block_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh, 
		    cbblock_t cb, sfsro_datares *rores, clnt_stat err);
  void indir_reply (time_t rqtime, nfscall *sbp, ref<const sfs_hash> fh, 
		     cbindir_t cb, sfsro_datares *rores, clnt_stat err);
  void read_blockres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
		      const char *d, size_t len);
  void read_block (const sfs_hash *fh, nfscall *sbp, cbblock_t cb);
  void read_indir (const sfs_hash *fh, nfscall *sbp, cbindir_t cb);
  void read_indirectres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
			 const sfsro_indirect *indirect);
  void read_doubleres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
		       const sfsro_indirect *indirect);
  void read_tripleres (size_t b, ref<const fattr3> fa, nfscall *sbp, 
		       const sfsro_indirect *indirect);
  void fh_lookup (size_t b, const sfsro_inode *ip, nfscall *sbp, 
		  const sfs_hash *fh);
  void read_file (const sfsro_inode *ip, nfscall *sbp, const sfs_hash *fh);
  void readdir_lookupres (nfscall *sbp, const sfsro_directory *dir);
  void readdirplus_lookupres (nfscall *sbp, readdirplus3res *nfsres,
			      const sfsro_directory *ip);
  void dir_lookupres (nfscall *sbp, const sfsro_directory *dir);
  void dir_lookupres_dir_attr (nfscall *sbp, 
			       ref<const sfs_hash> dir_fh,
			       ref<const sfs_hash> obj_fh,
			       const sfsro_inode *dir_ip);
  void dir_lookupres_obj_attr (nfscall *sbp, 
			       ref<const sfs_hash> obj_fh,
			       lookup3res *nfsres,
			       const sfsro_inode *obj_ip);
  void dir_lookup (const sfsro_inode *ip, nfscall *sbp, cbdirent_t cb);
  void readlinkinode_lookupres (nfscall *sbp, const sfsro_inode *ip);
  void readdirinode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			       const sfsro_inode *ip);
  void readdirplusinode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
				   const sfsro_inode *ip);
  void lookupinode_lookupres (nfscall *sbp, const sfsro_inode *ip);
  void accessinode_lookupres (nfscall *sbp, const sfsro_inode *ip);
  void readinode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			    const sfsro_inode *ip);
  void inode_lookupres (nfscall *sbp, ref<const sfs_hash> fh,
			const sfsro_inode *ip);
  void inode_lookup (const sfs_hash *fh, nfscall *sbp, cbinode_t cb);
  void nfs3_fsstat (nfscall *sbp);
  void nfs3_fsinfo (nfscall *sbp);

  void dir_lookup_parentres (nfscall *sbp, ref<const sfs_hash> dir_fh,
			     const sfs_hash *fh);

  void lookup_parent_rofh (nfscall *sbp, const sfs_hash *dir_fh,
			   ref<vec<str> > pathvec, cbparent_t cb);
  void lookup_parent_rofh_lookupres (nfscall *sbp, 
				     ref<vec <str> > pathvec,
				     cbparent_t cb,
				     const sfsro_inode *ip);
  void lookup_parent_rofh_lookupres2 (nfscall *sbp,
				      ref<vec <str> > pathvec,
				      cbparent_t cb,
				      const sfsro_directory *dir);
  void get_data (const sfs_hash *fh, 
		 callback<void, sfsro_datares *, clnt_stat>::ref cb);
  void get_data_cb(ptr<vec< sfsro_datares * > >ress,
		   sfsro_datares *res,		   int offset, ptr<int> recvd, 
		   callback<void, sfsro_datares *, clnt_stat>::ref cb,
		   clnt_stat err);    

  ///FED - mirror hack
  void updateMirrorDivision();
protected:
  server (const sfsserverargs &a)
    : sfsserver (a) {}

public:
  char IV[SFSRO_IVSIZE];

  ptr<aclnt> sfsroc;
  int numMirrors;

  static void sendreply (nfscall *sbp, void *res);
  int getreply (nfscall *sbp, sfsro_datares *rores, clnt_stat err,
		sfsro_data *data, const sfs_hash *fh);

  static void ro2nfs(const sfs_hash *fh, nfs_fh3 *nfh);
  static bool nfs2ro(nfs_fh3 *nfh, sfs_hash *fh);
  static void fh2fileid (const sfs_hash *fh, uint64 *fileid);

  bool setrootfh (const sfs_fsinfo *fsi);
  void setrootfh_1 (int start, int len, int fd);
  void dispatch (nfscall *sbp);
};


