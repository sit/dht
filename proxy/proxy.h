
#ifndef SFSNET_PROXY_H
#define SFSNET_PROXY_H

#include "sha1.h"
#include "bigint.h"
#include "dhash_common.h"
#include "dbfe.h"

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
  ptr<dbfe> ilog;

  str proxyhost;
  int proxyport;
  ptr<aclnt> proxyclnt;
  
  void proxy_connected (ptr<axprt_stream> x, int fd);
  void dispatch (svccb *sbp);

  void insert_cache_cb (dhash_insert_arg *arg,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_insert_cb (int options, svccb *sbp,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void local_insert_cb (bool disconnected, svccb *sbp,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_retrieve_cb (int options, svccb *sbp,
                          ptr<dhash_retrieve_res> res, clnt_stat err);
  void local_retrieve_cb (svccb *sbp, ptr<dhash_retrieve_res> res,
                          clnt_stat err);

public:
  proxygateway (ptr<axprt_stream> x, ptr<aclnt> local, ptr<dbfe> il,
                str proxyhost, int proxyport);
  ~proxygateway ();
};

static inline ref<dbrec>
my_id2dbrec (bigint id)
{
  char buf[sha1::hashsize];
  bzero (buf, sha1::hashsize);
  mpz_get_rawmag_be (buf, sha1::hashsize, &id);
  return New refcounted<dbrec> (buf, sha1::hashsize);
}

static inline bigint
my_dbrec2id (ref<dbrec> d)
{
  assert (d->len == sha1::hashsize);
  bigint n;
  mpz_set_rawmag_be(&n, d->value, sha1::hashsize);
  return n;
}

#endif

