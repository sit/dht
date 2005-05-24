#include "dhash_common.h"
#include "dhblock_replicated.h"

#include <configurator.h>

u_int
dhblock_replicated::num_replica ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("dhash.replica", v);
    assert (initialized);
  }
  return v;
}

int 
dhblock_replicated::process_download (blockID k, str frag)
{
  //XXX logic to keep track of other frags (if any) 
  // and vote if we get more than one content
  done_flag = true;
  result_str = frag;
  return 0;
}


str
dhblock_replicated::produce_block_data ()
{
  assert (done_flag);
  return result_str;
}

str 
dhblock_replicated::generate_fragment (ptr<dhash_block> block, int n)
{
  return block->data;
}
