
// Forward declarations.
class asrv;
class svccb;

class chord;
#include <route.h>

class dhashcli;
class dhash_block;

struct dhash_insert_res;
struct dhash_retrieve_res;

class dhashgateway : public virtual refcount
{
  ptr<asrv> clntsrv;
  ptr<dhashcli> dhcli;
  
  bool useproxy;
  str proxyhost;
  int proxyport;
  ptr<aclnt> proxyclnt;

  void dispatch (svccb *sbp);
  void insert_cache_cb (dhash_stat status, vec<chordID> path);
  void insert_cb (svccb *sbp, dhash_stat status, vec<chordID> path);
  void insert_cb_common (svccb *sbp, dhash_insert_res *res);
  void retrieve_cache_cb (svccb *sbp, ptr<dhash_block> block);
  void retrieve_cb (svccb *sbp, dhash_stat status, ptr<dhash_block> block,
                    route path);
  void retrieve_cb_common (svccb *sbp, dhash_retrieve_res *res);

  void proxy_connected (ptr<axprt_stream> x, ptr<chord> node, int fd);
  void proxy_retrieve_cb (int options, svccb *sbp,
                          ptr<dhash_retrieve_res> res, clnt_stat err);
  void proxy_insert_cb (int options, svccb *sbp,
                        ptr<dhash_insert_res> res, clnt_stat err);
  
public:
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt);
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt,
                str proxyhost, int proxyport);
  ~dhashgateway ();
};

