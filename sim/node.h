#ifndef INCL_NODE
#define INCL_NODE

#include "doc.h"
#include "finger.h"
#include "request.h"

typedef struct _node {
  ID             id;       // node identifier
#define ABSENT   0
#define PRESENT  1         
#define TO_LEAVE 2
  int            status;  
  int            fingerId; // id of the finger to be refreshed, where 
                           // id is of the form id + 2^{i-1}
  DocList       *docList;
  FingerList    *fingerList;
  RequestList   *reqList;
  struct _node *next;
} Node;

#endif /* INCL_NODE */
