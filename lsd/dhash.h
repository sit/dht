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

class dhashclient {

  ptr<axprt_stream> x;
  int do_caching;

  ptr<asrv> p2pclntsrv;


  void dispatch (svccb *sbp);
  void cache_on_path(dhash_insertarg *item, route path);

  void lookup_findsucc_cb (svccb *sbp, sfs_ID *n, struct timeval *tp,  sfs_ID succ, route path, sfsp2pstat err);
  void lookup_fetch_cb (svccb *sbp, dhash_res *res, struct timeval *tp, route p, sfs_ID *n,clnt_stat err);

  void insert_findsucc_cb (svccb *sbp, dhash_insertarg *item, sfs_ID succ, route path, sfsp2pstat err);
  void insert_store_cb (svccb *sbp, dhash_stat *res,clnt_stat err);

  void cache_store_cb(dhash_stat *res, clnt_stat err);

  void search_cb(sfs_ID myTarget, sfs_ID node, sfs_ID target, cbi cb);
  void search_cb_cb (dhash_stat *res, cbi cb, clnt_stat err);
 public:
  
  void set_caching(char c) { do_caching = c;};
  dhashclient (ptr<axprt_stream> x);
};

class dhash {

  int nreplica;

  dbfe *db;

  void dispatch (ptr<asrv> dhs, svccb *sbp);
  void fetchsvc_cb (svccb *sbp, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (store_cbstate *st, dhash_stat err);
  
  void fetch (sfs_ID id, cbvalue cb);
  void fetch_cb (cbvalue cb, ptr<dbrec> ret);
  void store (sfs_ID id, dhash_value data, store_status type, cbstore cb);
  void store_cb (cbstore cb, int stat);
  void cache_store_cb(dhash_res *res, clnt_stat err);
  
  dhash_stat key_status(sfs_ID n);

  void store_flush (sfs_ID key, dhash_stat value);
  void store_flush_cb (int err);
  void cache_flush (sfs_ID key, dhash_stat value);
  void cache_flush_cb (int err);

  void find_replica_cb (store_cbstate *st, sfs_ID s, net_address r, 
			sfsp2pstat status);
  void store_replica_cb(store_cbstate *st, dhash_stat *res, clnt_stat err);

  void act_cb(sfs_ID id, char action);

  ptr<dbrec> id2dbrec(sfs_ID id);

  vs_cache<sfs_ID, dhash_stat> key_store;
  vs_cache<sfs_ID, dhash_stat> key_cache;


 public:
  dhash (str dbname, int nreplica, int ss = 10000, int cs = 1000);
  void accept(ptr<axprt_stream> x);
};


#endif
