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
#define ABSENT   0
#define PRESENT  1
#define TO_LEAVE 2
  int           status;
  DocList       docList[MAX_NUM_DOCS];
  int           finger[NUM_BITS];
  int           successor;
  int           predecessor;
  struct _node *next;
} Node;

#endif /* INCL_NODE */
