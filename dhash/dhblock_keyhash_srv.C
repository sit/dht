#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"

#include <dbfe.h>
#include <dhblock_keyhash.h>
#include <dhblock_keyhash_srv.h>

dhblock_keyhash_srv::dhblock_keyhash_srv (ptr<vnode> node,
				          str desc,
					  str dbname,
					  dbOptions opts) :
  dhblock_replicated_srv (node, desc, dbname, opts, DHASH_KEYHASH)
{
}

bool
dhblock_keyhash_srv::is_block_stale (ref<dbrec> prev, ref<dbrec> d)
{
  long v0 = dhblock_keyhash::version (prev->value, prev->len);
  long v1 = dhblock_keyhash::version (d->value, d->len);
  if (v0 >= v1)
    return true;
  return false;
}

