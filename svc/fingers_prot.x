%#include "chord_prot.h"

program FINGERS_PROGRAM {
  version FINGERS_VERSION {
    void
    FINGERSPROC_NULL (void) = 0;

    chord_nodelistres
    FINGERSPROC_GETFINGERS (chordID) = 1;
    
    chord_nodelistextres
    FINGERSPROC_GETFINGERS_EXT (chordID) = 2;
  } = 1;
} = 344454;
