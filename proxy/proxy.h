
#ifndef SFSNET_PROXY_H
#define SFSNET_PROXY_H

#include "sha1.h"
#include "bigint.h"
#include "dhash_common.h"
#include "dbfe.h"

#define SYNC_TIME 30

class asrv;
class svccb;
class dhash_block;

struct dhash_insert_arg;
struct dhash_insert_res;
struct dhash_retrieve_res;

struct multiconn_args {
  vec<str> hosts;
  vec<int> ports;
  vec<int> responses;
  int timeout;
  cbi::ptr cb;
  str connected;
  timecb_t* delayed_connect;

  multiconn_args(vec<str> h, vec<int> p, int t, cbi::ptr c) {
    unsigned int i;
    hosts = h;
    ports = p;
    timeout = t;
    cb = c;
    connected = "";
    for(i=0; i<h.size(); i++) 
      responses.push_back(0);
  }
  void saw_response(int i) {
    responses[i] = 1;
  }
  bool all_responded() {
    for(unsigned int i=0; i<responses.size(); i++) {
      if (responses[i] == 0) return 0;
    } 
    return 1;
  }
};


class proxygateway : public virtual refcount
{
  ptr<asrv> clntsrv;
  ptr<dbfe> cache_db;
  ptr<dbfe> disconnect_log;

  vec<str> proxyhosts;
  vec<int> proxyports;
  ptr<aclnt> proxyclnt;
  
  void proxy_connected (ptr<axprt_stream> x, int fd);
  void dispatch (svccb *sbp);

  void insert_cache_cb (dhash_insert_arg *arg,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_insert_cb (int options, svccb *sbp,
                        ptr<dhash_insert_res> res, clnt_stat err);
  void proxy_retrieve_cb (int options, svccb *sbp,
                          ptr<dhash_retrieve_res> res, clnt_stat err);
  void local_retrieve_cb (svccb *sbp, ptr<dhash_retrieve_res> res,
                          clnt_stat err);
  void insert_to_localcache(chordID id, char* block, int32_t len, dhash_ctype ctype);
  void local_insert_done (bool disconnected, svccb *sbp);

public:
  proxygateway (ptr<axprt_stream> x, ptr<dbfe> cache, ptr<dbfe> dl,
                vec<str> proxyhosts, vec<int> proxyports);
  ~proxygateway ();
};

ptr<dhash_retrieve_res> block_to_res (ptr<dbrec> val, dhash_ctype ctype);
void multiconnect(vec<str> hosts, vec<int> ports, int timeout, cbi::ptr cb);

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

