#ifndef DOCS_INCL
#define DOCS_INCL

#define MAX_NUM_DOCS 50

#include "util.h"

typedef int DocId;

typedef struct _document {
  DocId             id;
  struct _document *next;
} Document;


typedef struct _docList {
  Document *head;
  int       size;
} DocList;

int insertDocInList(DocList *docList, Document *doc);
Document *findDocInList(DocList *docList, DocId docId);

#endif /* DOCS_INC */
