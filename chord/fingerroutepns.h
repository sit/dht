#ifndef _FINGERROUTEPNS_H_
#define _FINGERROUTEPNS_H_

#include "chord_impl.h"
#include <fingers_prot.h>
#include <fingerroute.h>

class finger_table;

class fingerroutepns : public fingerroute {
protected:
  fingerroutepns (ref<chord> _chordnode, 
		  ref<rpc_manager> _rpcm,
		  ref<location> _l);

 public:
  static ref<vnode> produce_vnode (ref<chord> _chordnode, 
				   ref<rpc_manager> _rpcm,
				   ref<location> _l);
};

#endif /* _FINGERROUTEPNS_H_ */
