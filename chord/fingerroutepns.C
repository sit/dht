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
  return New refcounted<fingerroutepns> (_chordnode, _rpcm, _l, wrap (&finger_table_pns::produce_finger_table));
}

fingerroutepns::fingerroutepns (ref<chord> _chordnode, 
				ref<rpc_manager> _rpcm,
				ref<location> _l,
				cb_fingertableproducer_t ftp) : fingerroute (_chordnode, _rpcm, _l, ftp) {};
