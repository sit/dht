
// Forward declarations.
class asrv;
class svccb;

class chord;
#include <route.h>

class dhash;
class dhashcli;
class dhash_block;

class dhashgateway {
  ptr<asrv> clntsrv;
  ptr<chord> clntnode;
  ptr<dhashcli> dhcli;
  dhash *dh;

  void dispatch (svccb *sbp);
  void insert_cb (svccb *sbp, dhash_stat status, chordID blockID);
  void retrieve_cb (svccb *sbp, dhash_stat status, ptr<dhash_block> block, route path);
  
public:
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt, dhash *dh,
		ptr<route_factory> f, bool do_cache = false,
		int ss_mode = 0);
};
