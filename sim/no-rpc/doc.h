#ifndef INCL_DOC
#define INCL_DOC

#include "def.h"

typedef struct _document {
  ID               id;
  struct _document *next;
} Document;


typedef struct _docList {
  Document *head;
  int       size;
} DocList;

#endif // INCL_DOC
