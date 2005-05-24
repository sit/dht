#include <dhblock_replicated.h>

class dhblock_noauth_srv : public dhblock_replicated_srv
{
  bool is_block_stale (ref<dbrec> prev, ref<dbrec> d) { return false; };
public:
  dhblock_noauth_srv (ptr<vnode> node, str desc, str dbname, dbOptions opts) :
    dhblock_replicated_srv (node, desc, dbname, opts, DHASH_NOAUTH)
  {
  };
};
