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

#include "vsc.h"

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
  int hops;

  retry_state (chordID ni, svccb *sbpi, chordID si,
	       route pi) :
    n (ni), sbp (sbpi), succ (si), path (pi) {};
};

class dhashclient {

  ptr<axprt_stream> x;
  ptr<asrv> clntsrv;
  ptr<chord> clntnode;

  int do_caching;
  int num_replicas;

  void dispatch (svccb *sbp);
  void cache_on_path(ptr<dhash_insertarg> item, route path);

  void lookup_findsucc_cb (svccb *sbp,
			   chordID succ, route path, chordstat err);
  void lookup_fetch_cb (dhash_res *res, retry_state *st,  clnt_stat err);
  void retry (retry_state *st, chordID p, net_address r, chordstat stat);
  
  void insert_findsucc_cb (svccb *sbp, ptr<dhash_insertarg> item, chordID succ, 
			   route path, chordstat err);
  void insert_store_cb(svccb *sbp,  dhash_storeres *res, 
		       clnt_stat err);

  void cache_store_cb(dhash_stat *res, clnt_stat err);

 public:
  
  void set_caching(char c) { do_caching = c;};
  void set_num_replicas(int num) { num_replicas = num; };

  dhashclient (ptr<axprt_stream> x, ptr<chord> clnt);
};

class dhash {

  int nreplica;
  dbfe *db;
  ptr<vnode> host_node;

  qhash<chordID, store_state, hashID> pst;

  void dhash_reply (long xid, unsigned long procno, void *res);

  void dispatch (unsigned long, chord_RPC_arg *, unsigned long);
  void fetchsvc_cb (long xid, dhash_fetch_arg *arg, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (long xid, dhash_insertarg *arg, dhash_stat err);
  
  void fetch (chordID id, cbvalue cb);
  void fetch_cb (cbvalue cb,  ptr<dbrec> ret);

  void store (dhash_insertarg *arg, cbstore cb);
  void store_cb(store_status type, chordID id, cbstore cb, int stat);
  void store_repl_cb (cbstore cb, dhash_stat err);
  bool store_complete (dhash_insertarg *arg);

  void replicate_key (chordID key, int degree, 
		      callback<void, dhash_stat>::ref cb);
  void replicate_key_succ_cb (chordID key, int degree_remaining, 
			      callback<void, dhash_stat>::ref cb,
			      vec<chordID> repls, chordID succ, chordstat err);
  void replicate_key_transfer_cb (chordID key, int degree_remaining, 
				  callback<void, dhash_stat>::ref cb,
				  chordID succ, 
				  vec<chordID> repls, dhash_stat err);

  void cache_store_cb(dhash_res *res, clnt_stat err);
  
  dhash_stat key_status(chordID n);
  void transfer_key (chordID to, chordID key, store_status stat, 
		     callback<void, dhash_stat>::ref cb);
  void transfer_fetch_cb (chordID to, chordID key, store_status stat, 
			  callback<void, dhash_stat>::ref cb,
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

  char responsible(chordID& n);

  void printkeys ();
  void printkeys_walk (chordID k);
  void printcached_walk (chordID k);

  ptr<dbrec> id2dbrec(chordID id);

  vs_cache<chordID, dhash_stat> key_store;
  vs_cache<chordID, dhash_stat> key_cache;
  
  chordID pred;
  vec<chordID> replicas;

 public:
  dhash (str dbname, ptr<vnode> node, 
	 int nreplica = 0, int ss = 10000, int cs = 1000);
  void accept(ptr<axprt_stream> x);
};


#endif
