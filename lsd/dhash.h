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

/*
 *
 * dhash.h
 *
 * Include file for the distributed hash service
 */

typedef callback<void, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void, dhash_stat>::ptr cbstat;

#define DHASH_STORE "/tmp/dhash.db"

class dhashclient {
  ptr<axprt_stream> x;
  ptr<asrv> p2pclntsrv;

  void dispatch (svccb *sbp);
  void lookup_findsucc_cb (svccb *sbp, sfs_ID *n, sfs_ID succ, route path, sfsp2pstat err);
  void lookup_connect_cb (svccb *sbp, sfs_ID *n, ptr<axprt_stream> x);
  void lookup_fetch_cb (svccb *sbp, dhash_res *res, clnt_stat err);

  void insert_findsucc_cb (svccb *sbp, dhash_insertarg *item, sfs_ID succ, route path, sfsp2pstat err);
  void insert_connect_cb (svccb *sbp, dhash_insertarg *item, ptr<axprt_stream> x);
  void insert_store_cb (svccb *sbp, dhash_stat *res, clnt_stat err);

  
 public:
  dhashclient (ptr<axprt_stream> x);
};

class dhash {
  ptr<asrv> dhashsrv;
  dbfe *db;

  void dispatch (svccb *sbp);
  void fetchsvc_cb (svccb *sbp, ptr<dbrec> val, dhash_stat err);
  void storesvc_cb (svccb *sbp, dhash_stat err);
  
  void fetch (sfs_ID id, cbvalue cb);
  void fetch_cb (cbvalue cb, ptr<dbrec> ret);
  void store (sfs_ID id, dhash_value data, cbstat cb);
  void store_cb (cbstat cb, int stat);

  ptr<dbrec> id2dbrec(sfs_ID id);

 public:
  dhash (ptr<axprt_stream> x);
};


#endif
