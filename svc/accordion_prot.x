%#include "chord_types.h"
%#include "recroute_prot.h"

struct accordion_fillgap_arg {
  chord_node_wire end;
  unsigned para; //sender's parallelism, used to determine the freshness of the replied routing entries
};

program ACCORDION_PROGRAM {
  version ACCORDION_VERSION {
    void
    ACCORDIONPROC_NULL (chordID) = 0; 
    chord_nodelistres
    ACCORDIONPROC_FILLGAP (accordion_fillgap_arg) = 1;
    chord_nodelistres
    ACCORDIONPROC_LOOKUP (recroute_route_arg) = 2;
    void
    ACCORDIONPROC_LOOKUP_COMPLETE (recroute_complete_arg) = 3;
    chord_nodelistextres
    ACCORDIONPROC_GETFINGERS_EXT (chordID) = 4;
  } = 1;
} = 344446;
