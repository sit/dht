%#include "bigint.h"
%#include "chord_types.h"

enum rpcstat {
  DORPC_OK = 0,
  DORPC_MARSHALLERR = 1,
  DORPC_UNKNOWNNODE = 2,
  DORPC_NOHANDLER = 3
};

struct dorpc_arg {
  chord_node_wire dest;
  chord_node_wire src;
  
  u_int64_t send_time;

  int32_t progno;
  int32_t procno;
  opaque  args<>;
};

struct dorpc_successres {
  chord_node_wire src;
  u_int64_t send_time_echo;
  opaque results<>;
};

union dorpc_res switch (rpcstat status) {
 case DORPC_OK:
   dorpc_successres resok;
 default:
   void;
};

program TRANSPORT_PROGRAM {
  version TRANSPORT_VERSION {
    void
    TRANSPORTPROC_NULL (void) = 0;
    
    dorpc_res
    TRANSPORTPROC_DORPC (dorpc_arg) = 1;
  } = 1;
} = 344451;
