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
  sfs_ID n;
  svccb *sbp;
  sfs_ID succ;
  route path;
  searchcb_entry *scb;
  retry_state (sfs_ID ni, svccb *sbpi, sfs_ID si,
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

  void lookup_findsucc_cb (svccb *sbp,
			   searchcb_entry *scb,
			   sfs_ID succ, route path, sfsp2pstat err);
  void lookup_fetch_cb (dhash_res *res, retry_state *st,  clnt_stat err);
  void retry (retry_state *st, sfs_ID p, net_address r, sfsp2pstat stat);
  
  void insert_findsucc_cb (svccb *sbp, ptr<dhash_insertarg> item, sfs_ID succ, 
			   route path, sfsp2pstat err);
  void insert_store_cb(svccb *sbp,  dhash_storeres *res, 
		       clnt_stat err);

  void cache_store_cb(dhash_stat *res, clnt_stat err);

  void search_cb(sfs_ID myTarget, sfs_ID node, sfs_ID target, cbi cb);
  void search_cb_cb (dhash_stat *res, cbi cb, clnt_stat err);
 public:
  
  void set_caching(char c) { do_caching = c;};
  void set_num_replicas(int num) { num_replicas = num; };

  dhashclient (ptr<axprt_stream> x);
};

class dhash {

  int nreplica;

  dbfe *db;

  qhash<sfs_ID, store_state, hashID> pst;

  void dispatch (ptr<asrv> dhs, svccb *sbp);
  void fetchsvc_cb (svccb *sbp, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (svccb *sbp, dhash_stat err);
  
  void fetch (sfs_ID id, cbvalue cb);
  void fetch_cb (cbvalue cb,  ptr<dbrec> ret);

  void store (dhash_insertarg *arg, cbstore cb);
  void store_cb(store_status type, sfs_ID id, cbstore cb, int stat);
  void store_repl_cb (cbstore cb, dhash_stat err);
  bool store_complete (dhash_insertarg *arg);

  void replicate_key (sfs_ID key, int degree, callback<void, dhash_stat>::ref cb);
  void replicate_key_succ_cb (sfs_ID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
			      vec<sfs_ID> repls, sfs_ID succ, sfsp2pstat err);
  void replicate_key_transfer_cb (sfs_ID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
				  sfs_ID succ, vec<sfs_ID> repls, dhash_stat err);

  void cache_store_cb(dhash_res *res, clnt_stat err);
  
  dhash_stat key_status(sfs_ID n);
  void transfer_key (sfs_ID to, sfs_ID key, store_status stat, callback<void, dhash_stat>::ref cb);
  void transfer_fetch_cb (sfs_ID to, sfs_ID key, store_status stat, callback<void, dhash_stat>::ref cb,
			  ptr<dbrec> data, dhash_stat err);
  void transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, clnt_stat err);

  void store_flush (sfs_ID key, dhash_stat value);
  void store_flush_cb (int err);
  void cache_flush (sfs_ID key, dhash_stat value);
  void cache_flush_cb (int err);

  void act_cb(sfs_ID id, char action);
  void walk_cb(sfs_ID succ, sfs_ID id, sfs_ID key);

  void transfer_key_cb (sfs_ID key, dhash_stat err);
  void fix_replicas_cb (sfs_ID id, sfs_ID k);
  void fix_replicas_transfer_cb (dhash_stat err);
  void rereplicate_cb (sfs_ID k);
  void rereplicate_replicate_cb (dhash_stat err);

  char responsible(sfs_ID n);

  void printkeys();
  void printkeys_walk (sfs_ID k);
  void printcached_walk (sfs_ID k);

  ptr<dbrec> id2dbrec(sfs_ID id);

  vs_cache<sfs_ID, dhash_stat> key_store;
  vs_cache<sfs_ID, dhash_stat> key_cache;
  
  sfs_ID pred;
  vec<sfs_ID> replicas;

 public:
  dhash (str dbname, int nreplica, int ss = 10000, int cs = 1000);
  void accept(ptr<axprt_stream> x);
};


#endif
