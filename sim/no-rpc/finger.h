#ifndef INCL_FINGER
#define INCL_FINGER

#include "def.h"

typedef struct _finger {
  ID     id;
  double last;   // last time when finger was refreshed
  double expire; // time by which the finger will expire;
                 // this plays the role of a time-out and 
                 // it is set when the node initiates a request
                 // and send it to node id
                // (value 0 is used for infinity)
  struct _finger *next;
} Finger;


typedef struct _fingerList {
  Finger  *head;
  Finger  *tail;
  int      size;
} FingerList;

#endif // INCL_FINGER
