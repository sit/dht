#include <dhblock_replicated_srv.h>

class dhblock_keyhash_srv : public dhblock_replicated_srv
{
  bool is_block_stale (str prev, str d);
public:
  dhblock_keyhash_srv (ptr<vnode> node, str desc, str dbname, str dbext);
};
