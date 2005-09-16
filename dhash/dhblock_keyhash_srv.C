#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"

#include <dbfe.h>
#include <dhblock_keyhash.h>
#include <dhblock_keyhash_srv.h>

dhblock_keyhash_srv::dhblock_keyhash_srv (ptr<vnode> node,
					  ptr<dhashcli> cli,
				          str desc,
					  str dbname,
					  str dbext) :
  dhblock_replicated_srv (node, cli, desc, dbname, dbext, DHASH_KEYHASH)
{
}

bool
dhblock_keyhash_srv::is_block_stale (str prev, str d)
{
  long v0 = dhblock_keyhash::version (prev.cstr (), d.len ());
  long v1 = dhblock_keyhash::version (d.cstr (), d.len ());
  if (v0 >= v1)
    return true;
  return false;
}

