#include "fingerroutepns.h"

#include <id_utils.h>
#include <misc_utils.h>
#include "finger_table.h"
#include "finger_table_pns.h"
#include "fingerroute.h"
#include "pred_list.h"
#include "succ_list.h"

#include <location.h>
#include <locationtable.h>

ref<vnode>
fingerroutepns::produce_vnode (ref<chord> _chordnode,
			       ref<rpc_manager> _rpcm,
			       ref<location> _l)
{
  return New refcounted<fingerroutepns> (_chordnode, _rpcm, _l);
}

fingerroutepns::fingerroutepns (ref<chord> _c,
				ref<rpc_manager> _r,
				ref<location> _l)
  : fingerroute (_c, _r, _l, wrap (&finger_table_pns::produce_finger_table))
{
}
