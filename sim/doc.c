#include <stdlib.h>
#include "incl.h"

extern Node *NodeHashTable[HASH_SIZE];

void findDocument_return(Node *n, Stack *stack);
void insertDocument_return(Node *n, Stack *stack);

int insertDocInList(DocList *docList, Document *doc);
Document *findDocInList(DocList *docList, DocId docId);
void updateDocList(Node *n);


void findDocument(Node *n, int *docId)
{
  Stack *stack;

  if (n->status != PRESENT)
    return;

  stack = pushStack(NULL, newStackItem(n->id, findDocument_return));
  stack->data.key = *docId;
  CALL(n->id, findSuccessor_entry, stack);
}

void findDocument_return(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  Node  *n1  = getNode(top->ret.nid);
  int docId  = top->data.key;
  Document *doc;


  if (!n1) {
    printf("findNode: node %d has been deleted in the meantime!\n", 
	   top->ret.nid);
    return;
  }

  if (findDocInList(n1->docList, docId))
    printf("%f Document %d found on node %d\n", Clock, docId, n1->id);
  else 
    printf("%f Document %d NOT found on node %d\n", Clock, docId, n1->id);

  RETURN(n->id, stack = popStack(stack));
}


void insertDocument(Node *n, int *docId)
{
  Stack *stack;

  stack = pushStack(NULL, newStackItem(n->id, insertDocument_return));
  stack->data.key = *docId;
  CALL(n->id, findSuccessor_entry, stack);
}
  

void insertDocument_return(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  Node  *n1  = getNode(top->ret.nid);
  int docId  = top->data.key;
  Document *doc;

  if (!n1) {
    printf("insertReply: node has been deleted in the meantime!\n");
    free(doc);
    KILL_REQ(stack);
  } else {
    /* allocate space for new document */
    if ((doc = (Document *)calloc(1, sizeof(Document))) == NULL)
      panic("insertReply: memory allocation error!\n");
    
    doc->id = docId;
    
    if (!insertDocInList(n1->docList, doc)) {
      /* no room to insert document */
      printf("cannot insert document %x at node %x\n", doc->id, n1->id); 
      free(doc);
      return;
    } else
      printf("The document %d inserted on node %d\n", docId, n1->id);
  }

  RETURN(n->id, stack = popStack(stack));
}


/* insert specified document at the head of document list 
 *   return TRUE if document successfully inserted, and FALSE otherwise
 *   if document already in the list, do nothing and return TRUE
 */
int insertDocInList(DocList *docList, Document *doc)
{
  Document *tmp;

  if (docList->size >= MAX_NUM_DOCS) {
    /* no room available in document list */
    printf("======\n");
    return FALSE;
  }

  /* insert document at the head of the list */
  tmp = docList->head;
  docList->head = doc;
  doc->next = tmp;

  return TRUE;
}

/* find document with key docId */
Document *findDocInList(DocList *docList, DocId docId)
{
  Document *doc;

  for (doc = docList->head; doc; doc = doc->next)
    if (doc->id == docId)
      return doc;

  return NULL;
}


void *freeDocList(Node *n)
{
  Document *doc, *tmp;

  while (doc = n->docList->head) {
    tmp = n->docList->head;
    n->docList->head = n->docList->head->next;
    free(tmp);
  }
}
  


/* move documents from n's successor to n */
void updateDocList(Node *n)
{
  Document *doc, *d;
  Node     *s = getNode(n->successor);
  int       flag = FALSE;

  if (n == s || n->status != PRESENT)
    return;

  if (!s) {
    printf("updateDocList: node has been deleted in the meantime!\n");
    return;
  }


  /* move all documents x stored on s, and that are not between n and s,
   * to n, i.e., n is now closest to documents x than s
   */ 
  doc = s->docList->head;
  while (doc) {
    /* check whether doc is  [n, s) ... */ 
    if (!between(doc->id, n->id, s->id, NUM_BITS) && doc->id != s->id) {
      /* ... if not, move document from s' document list to n's 
       * document list 
       */
      s->docList->head = doc->next;
      insertDocInList(n->docList, doc);
    } else
      break;
    doc = s->docList->head;
  }
  
  if (!(d = doc))
    return;

  while (doc = d->next) {
    if (!between(doc->id, n->id, s->id, NUM_BITS) && doc->id != s->id) {
      /* move document from s' document list to n's document list */
      d->next = d->next->next;
      insertDocInList(n->docList, doc);
    } else
      d = d->next;
  }
}


/* move documents from n1 to n2 */
void moveDocList(Node *n1, Node *n2)
{
  Document *doc;

  if (n1 == n2)
    return;

  if (!n1) {
    printf("moveDocList: node has been deleted in the meantime!\n");
    return;
  }


  /* move all documents x stored on s, and that are not between n and s,
   * to n, i.e., n is now closest to documents x than s
   */ 
  doc = n1->docList->head;
  while (doc) {
    n1->docList->head = doc->next;
    insertDocInList(n2->docList, doc);
    doc = n1->docList->head;
  }
}





               
