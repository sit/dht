#ifndef INCL_NODE
#define INCL_NODE

typedef int DocId;

typedef struct _document {
  DocId             id;
  struct _document *next;
} Document;


typedef struct _docList {
  Document *head;
  int       size;
} DocList;

typedef struct _node {
  int           id;    /* node identifier */
  int           status;
  DocList       docList[MAX_NUM_DOCS];
  int           successorId[NUM_BITS];
  int           predecessorId[NUM_BITS];
  struct _node *next;
} Node;

#endif /* INCL_NODE */
