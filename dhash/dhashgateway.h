
// Forward declarations.
class asrv;
class svccb;

class chord;
#include <route.h>

class dhashcli;
class dhash_block;

class dhashgateway {
  ptr<asrv> clntsrv;
  ptr<dhashcli> dhcli;

  void dispatch (svccb *sbp);
  void insert_cache_cb (dhash_stat status, vec<chordID> path);
  void insert_cb (svccb *sbp, dhash_stat status, vec<chordID> path);
  void retrieve_cache_cb (svccb *sbp, ptr<dhash_block> block);
  void retrieve_cb (svccb *sbp, dhash_stat status, ptr<dhash_block> block,
                    route path);
  
public:
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt);
};
