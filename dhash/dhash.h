#ifndef _DHASH_H_
#define _DHASH_H_

#include <arpc.h>
#include <async.h>
#include <dhash_prot.h>
#include <chord_prot.h>
#include <dbfe.h>
#include <callback.h>
#include <refcnt.h>
#include <chord.h>
#include <qhash.h>
#include <sys/time.h>
#include <chord.h>
/*
 *
 * dhash.h
 *
 * Include file for the distributed hash service
 */

template<class KEY, class VALUE>
class vs_cache {
  struct cache_entry {
    vs_cache    *c;
     KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (vs_cache<KEY, VALUE> *cc,
	        KEY &kk,  VALUE *vv)
      : c (cc), k (kk)
    {      v = *vv;
    c->lrulist.insert_tail (this);
    c->entries.insert (this);
    c->num_cache_entries++;
    while (c->num_cache_entries > implicit_cast<u_int> (c->max_cache_entries)) {
      if (c->fcb) (c->fcb) (c->lrulist.first->k, c->lrulist.first->v);
      delete c->lrulist.first;
    }
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

  typedef callback<void, KEY, VALUE>::ptr flushcb_t;
  
private:
  friend class cache_entry;   //XXX hashid is a hack that ruins the generic nature of the cache
  ihash<KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink, hashID> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;
  u_int max_cache_entries;
  flushcb_t fcb;
public:
  vs_cache (u_int max_entries = 250) : num_cache_entries (0), 
    max_cache_entries (max_entries), 
    fcb (NULL) { };

  ~vs_cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  void enter ( KEY& kk,  VALUE *vv)
    {
      cache_entry *ad = entries[kk];
      if (!ad)
	vNew cache_entry (this, kk, vv);
      else 
	ad->touch ();
    }
  
  void remove ( KEY& k) 
    {
      
      entries.remove(entries[k]);
    }

   VALUE *lookup ( KEY& kk)
    {
      cache_entry *ad = entries[kk];
      if (ad) {
	ad->touch ();
	return &ad->v;
      }
      return NULL;
    }
  
   void traverse ( callback<void, KEY>::ref cb ) 
     {
       cache_entry *e = entries.first ();
       while (e) 
	 {
	   cb (e->k);
	   e = entries.next (e);
	 }
     }

   VALUE *peek ( KEY& k) {
    cache_entry *ad = entries[k];
    if (ad) {
      return &ad->v;
    }
    return NULL;
  }
  
  void set_flushcb (flushcb_t cb ) {
    fcb = cb;
  }
};


struct store_cbstate;

typedef callback<void, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void, struct store_cbstate *,dhash_stat>::ptr cbstat;
typedef callback<void,dhash_stat>::ptr cbstore;

struct store_cbstate {
  svccb *sbp;
  int nreplica;
  int r;
  dhash_insertarg *item;
  cbstat cb;
  store_cbstate (svccb *sbpi, int ni, dhash_insertarg *ii, cbstat cbi) :
    sbp (sbpi), nreplica (ni), item (ii), cb (cbi) 
  { r = nreplica + 1; };
};

struct store_state {
  unsigned int read;
  unsigned int size;
  char *buf;
  
  store_state (int z) : read(0), size(z), buf(New char[z]) {};
};

struct retry_state {
  chordID n;
  svccb *sbp;
  chordID succ;
  route path;
  searchcb_entry *scb;
  retry_state (chordID ni, svccb *sbpi, chordID si,
	       route pi, searchcb_entry *scbi) :
    n (ni), sbp (sbpi), succ (si), path (pi), scb (scbi) {};
};

class dhashclient {

  ptr<axprt_stream> x;
  int do_caching;
  int num_replicas;

  ptr<asrv> p2pclntsrv;


  void dispatch (svccb *sbp);
  void cache_on_path(ptr<dhash_insertarg> item, route path);

  void lookup_findsucc_cb (svccb *sbp, chordID n,
			   searchcb_entry *scb,
			   chordID succ, route path, chordstat err);
  void lookup_fetch_cb (dhash_res *res, retry_state *st,  clnt_stat err);
  void retry (retry_state *st, chordID p, net_address r, chordstat stat);
  
  void insert_findsucc_cb (svccb *sbp, ptr<dhash_insertarg> item, chordID succ, 
			   route path, chordstat err);
  void insert_store_cb(svccb *sbp,  dhash_storeres *res, 
		       clnt_stat err);

  void cache_store_cb(dhash_stat *res, clnt_stat err);

  void search_cb(chordID myTarget, chordID node, chordID target, cbi cb);
  void search_cb_cb (dhash_stat *res, cbi cb, clnt_stat err);
 public:
  
  void set_caching(char c) { do_caching = c;};
  void set_num_replicas(int num) { num_replicas = num; };

  dhashclient (ptr<axprt_stream> x);
};

class dhash {

  int nreplica;

  dbfe *db;

  qhash<chordID, store_state, hashID> pst;

  void dispatch (ptr<asrv> dhs, svccb *sbp);
  void fetchsvc_cb (svccb *sbp, chordID n, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (svccb *sbp, dhash_stat err);
  
  void fetch (chordID id, cbvalue cb);
  void fetch_cb (cbvalue cb,  ptr<dbrec> ret);

  void store (chordID id, dhash_value data, store_status type, cbstore cb);
  void store_cb(store_status type, chordID id, cbstore cb, int stat);
  void store_repl_cb (cbstore cb, dhash_stat err);
  bool store_complete (dhash_insertarg *arg);

  void replicate_key (chordID key, int degree, callback<void, dhash_stat>::ref cb);
  void replicate_key_succ_cb (chordID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
			      vec<chordID> repls, chordID succ, chordstat err);
  void replicate_key_transfer_cb (chordID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
				  chordID succ, vec<chordID> repls, dhash_stat err);

  void cache_store_cb(dhash_res *res, clnt_stat err);
  
  dhash_stat key_status(chordID n);
  void transfer_key (chordID to, chordID key, store_status stat, callback<void, dhash_stat>::ref cb);
  void transfer_fetch_cb (chordID to, chordID key, store_status stat, callback<void, dhash_stat>::ref cb,
			  ptr<dbrec> data, dhash_stat err);
  void transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, clnt_stat err);

  void store_flush (chordID key, dhash_stat value);
  void store_flush_cb (int err);
  void cache_flush (chordID key, dhash_stat value);
  void cache_flush_cb (int err);

  void act_cb(chordID id, char action);
  void walk_cb(chordID succ, chordID id, chordID key);

  void transfer_key_cb (chordID key, dhash_stat err);
  void fix_replicas_cb (chordID id, chordID k);
  void fix_replicas_transfer_cb (dhash_stat err);
  void rereplicate_cb (chordID k);
  void rereplicate_replicate_cb (dhash_stat err);

  char responsible(chordID n);

  ptr<dbrec> id2dbrec(chordID id);

  vs_cache<chordID, dhash_stat> key_store;
  vs_cache<chordID, dhash_stat> key_cache;
  
  chordID pred;
  vec<chordID> replicas;

 public:
  dhash (str dbname, int nreplica, int ss = 10000, int cs = 1000);
  void accept(ptr<axprt_stream> x);
};


#endif
