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

typedef callback<void, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void, dhash_stat>::ptr cbstat;


class dhashclient {

  int do_caching;
  ptr<axprt_stream> x;
  ptr<asrv> p2pclntsrv;

  void dispatch (svccb *sbp);
  void cache_on_path(dhash_insertarg *item, route path);

  void lookup_findsucc_cb (svccb *sbp, sfs_ID *n, struct timeval *tp,  sfs_ID succ, route path, sfsp2pstat err);
  void lookup_fetch_cb (svccb *sbp, dhash_res *res, struct timeval *tp, clnt_stat err);

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

  static const int nreplica = 5;

  dbfe *db;

  void dispatch (ptr<asrv> dhs, svccb *sbp);
  void fetchsvc_cb (svccb *sbp, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (svccb *sbp, dhash_stat err);
  
  void fetch (sfs_ID id, cbvalue cb);
  void fetch_cb (cbvalue cb, ptr<dbrec> ret);
  void store (sfs_ID id, dhash_value data, store_status type, cbstat cb);
  void store_cb (cbstat cb, int stat);
  void cache_store_cb(dhash_res *res, clnt_stat err);
  
  void act_cb(sfs_ID id, char action);

  ptr<dbrec> id2dbrec(sfs_ID id);

  qhash<sfs_ID, int, hashID> key_status;

 public:
  dhash (str dbname);
  void accept(ptr<axprt_stream> x);
};


#endif
