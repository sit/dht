#ifndef _SFSROSD_H_
#define _SFSROSD_H_

#include <sfsro_prot.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include <sfsdb.h>
#include "vec.h"

extern sfs_connectres cres;
extern sfs_fsinfo fsinfores;
extern sfsrodb db;
extern vec<sfsro_mirrorarg> mirrors;
extern int s;
extern int l;

class sfsroclient {
  ptr<axprt_stream> x;
  ptr<asrv> rosrv;
  ptr<asrv> sfssrv;

  ref<bool> destroyed;

  bool unixauth;
  uid_t uid;

  bool authid_valid;
  sfs_hash authid;


  void updatemirrorinfo();
  void addmirror(sfsro_mirrorarg *arg);

  void dispatch (ref<bool> b, svccb *sbp);
  void getdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d);

  void proxygetdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d);
  void proxygetdata_cb_connect(svccb *sbp, sfsro_datares *res , int fd);
  void proxygetdata_cb_call(svccb *sbp, sfsro_datares *res, clnt_stat err);

public:
  sfsroclient (ptr<axprt_stream> x, const authunix_parms *aup = NULL);
  ~sfsroclient ();
};

#endif _SFSROSD_H_
