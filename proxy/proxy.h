
#ifndef SFSNET_PROXY_H
#define SFSNET_PROXY_H

#include "dhash_common.h"

class asrv;
class svccb;
class dhash_block;

struct dhash_insert_arg;
struct dhash_insert_res;
struct dhash_retrieve_res;

class proxygateway : public virtual refcount
{
  ptr<asrv> clntsrv;
  ptr<aclnt> local;
  
  str proxyhost;
  int proxyport;
  ptr<aclnt> proxyclnt;
  
  void proxy_connected (ptr<axprt_stream> x, ptr<aclnt> l, int fd);
  void dispatch (svccb *sbp);

  void insert_cache_cb (dhash_insert_arg *arg,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_insert_cb (int options, svccb *sbp,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void local_insert_cb (svccb *sbp, ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_retrieve_cb (int options, svccb *sbp,
                          ptr<dhash_retrieve_res> res, clnt_stat err);
  void local_retrieve_cb (svccb *sbp, ptr<dhash_retrieve_res> res,
                          clnt_stat err);

public:
  proxygateway (ptr<axprt_stream> x, ptr<aclnt> local,
                str proxyhost, int proxyport);
  ~proxygateway ();
};

#endif

