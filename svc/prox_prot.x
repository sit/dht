%#include "chord_prot.h"

struct prox_gettoes_arg {
  int32_t level;
};

struct prox_findtoes_arg {
  int32_t level;
  chord_node_wire n;
};

program PROX_PROGRAM {
  version PROX_VERSION {
    void
    PROXPROC_NULL (void) = 0;

    chord_nodelistextres
    PROXPROC_GETTOES (prox_gettoes_arg) = 1;
    
    chord_nodelistres 
    PROXPROC_FINDTOES (prox_findtoes_arg) = 2;
  } = 1;
} = 344455;
