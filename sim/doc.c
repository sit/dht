#include <stdlib.h>
#include "incl.h"

int insertDocInList(DocList *docList, Document *doc);
Document *findDocInList(DocList *docList, DocId docId);
void updateDocList(Node *n);
void findReply(Node *n, FindArgStruct *p);
void insertReply(Node *n, FindArgStruct *p);

void findDocument(Node *n, int *docId)
{
  findSuccessor(n, lookupClosestPredecessor(n, *docId), 
		*docId, findReply);
}

void findReply(Node *n, FindArgStruct *p)
{
  Node  *n1  = getNode(p->replyId);
  int docId  = p->queryId;
  Document *doc;

  if (findDocInList(n1->docList, docId))
    printf("Document %d found on node %d (%d)\n", p->queryId, 
	   p->replyId, n1->id);
  else 
    printf("Document %d NOT found on node %d (%d)\n", p->queryId, 
	   p->replyId, n1->id);
    
}


void insertDocument(Node *n, int *docId)
{
  findSuccessor(n, lookupClosestPredecessor(n, *docId), 
		*docId, insertReply);
}
  

void insertReply(Node *n, FindArgStruct *p)
{
  Node  *n1  = getNode(p->replyId);
  int docId  = p->queryId;
  Document *doc;

  /* allocate space for new document */
  if ((doc = (Document *)calloc(1, sizeof(Document))) == NULL)
    panic("insertReply: memory allocation error!\n");

  doc->id = docId;

  if (!insertDocInList(n1->docList, doc)) {
    /* no room to insert document */
    printf("cannot insert document %x at node %x\n", doc->id, n1->id); 
    free(doc);
    return;
  }
  printf("The document %d inserted on node %d(%d)\n", p->queryId, 
	p->replyId, n1->id);
}


/* insert specified document at the head of document list 
 *   return TRUE if document successfully inserted, and FALSE otherwise
 *   if document already in the list, do nothing and return TRUE
 */
int insertDocInList(DocList *docList, Document *doc)
{
  Document *tmp;

  if (docList->size >= MAX_NUM_DOCS) 
    /* no room available in document list */
    return FALSE;

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
  Node     *s = getNode(getSuccessor(n, 0));
  int       flag = FALSE;

  /* move all documents x stored on s, and that are not between n and s,
   * to n, i.e., n is now closest to documents x than s
   */ 
  doc = s->docList->head;
  while (doc) {
    /* check whether doc is between n and s ... */ 
    if (!between(doc->id, n->id, s->id, NUM_BITS)) {
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
    if (!between(doc->id, n->id, s->id, NUM_BITS)) {
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





               
