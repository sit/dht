#include <stdio.h>
#include <malloc.h>
#include <math.h>

#include "incl.h"

void evictFinger(FingerList *fList);
void cleanFingerList(FingerList *fList);
void removeFinger(FingerList *fList, Finger *f);


Finger *newFinger(ID id)
{
  Finger *finger;

  /* allocate space for new finger */
  if ((finger = (Finger *)calloc(1, sizeof(Finger))) == NULL)
    panic("newFinger: memory allocation error!\n");
    
  finger->id     = id;
  finger->last   = Clock;
  finger->expire = MAX_TIME;
}

ID getSuccessor(Node *n)
{
  if (n->fingerList->head)
    return n->fingerList->head->id;
  else
    return n->id;
}

ID getPredecessor(Node *n)
{
  if (n->fingerList->head)
    return n->fingerList->tail->id;
  else
    return n->id;
}

// insert new finger in the list; if the finger is already in the
// list refresh it
void insertFinger(Node *n, ID id)
{
  FingerList *fList = n->fingerList;
  Finger *f, *ftemp;

  if (n->id == id)
    return;

  // remove finger entries that have expired
  cleanFingerList(fList);

  // optimization: if n's predecessor changes 
  // announce n's previous predecessor
  if (fList && between(id, getPredecessor(n), n->id, NUM_BITS)) {
    Request *r;
    ID       pred = getPredecessor(n);

    r = newRequest(id, REQ_TYPE_PRED, REQ_STYLE_RECURSIVE, pred);
    r->done = TRUE;
    r->x = r->sender = r->succ = id;
 
    genEvent(pred, insertRequest, (void *)r, Clock + intExp(AVG_PKT_DELAY));
  }    

  if (f = getFinger(fList, id)) {
    // just refresh finger f
    f->last   = Clock;
    f->expire = MAX_TIME;
    return;
  }

  // make room for the new finger
  if (fList->size == MAX_NUM_FINGERS)
    evictFinger(fList);

  if (!fList->size) {
    // fList empty; insert finger in fList
    fList->size++;
    fList->head = fList->tail = newFinger(id);
    updateDocList(n);
    return;
  }

  if (between(id, n->id, fList->head->id, NUM_BITS)) {
    ftemp = fList->head;
    fList->head = newFinger(id);
    fList->head->next = ftemp;
    fList->size++;
    updateDocList(n);
    return;
  }

  for (f = fList->head; f; f = f->next) {

    if (!f->next) {
      fList->tail = f->next = newFinger(id);
      fList->size++;
      break;
    }

    if (between(id, f->id, f->next->id, NUM_BITS)) {
      // insert new finger just after f
      ftemp = f->next;
      f->next = newFinger(id);
      f->next->next = ftemp;
      fList->size++;
      break;
    }
  }
}


// search and return finger id from fList
Finger *getFinger(FingerList *fList, ID id)
{
  Finger *f;

  for (f = fList->head; f; f = f->next)
    if (f->id == id)
      return;
  
  return NULL;
}


// Remove finger
void removeFinger(FingerList *fList, Finger *f)
{
  Finger *f1;

  if (fList->head == f) {
    fList->size--;
    fList->head = fList->head->next;
    free(f);
    if (!fList->head)
      fList->tail = NULL;
    return;
  }

  for (f1 = fList->head; f1; f1 = f1->next) {
    if (f1->next == f) {
      f1->next = f1->next->next;
      fList->size--;
      if (!f1->next)
	fList->tail = f1;
      free(f);
    }
  }
}


// remove fingers whose entries expired;
// this happens when an initiator forwards
// an iterative request to a node, but doesn't
// hear back from it
void cleanFingerList(FingerList *fList)
{
  Finger *f, *ftemp;

  if (!fList->head)
    return;

  for (f = fList->head; f; ) {
    if (f->expire < Clock) {
      removeFinger(fList, f);
      f = fList->head;
    } else
      f = f->next;
  }
}


// evict the finger that was not refrerred for the longest time
// (implement LRU)
void evictFinger(FingerList *fList)
{
  Finger *f, *ftemp;
  double time = MAX_TIME;

  for (f = fList->head; f; f = f->next) {
    if (f->last < time)
      time = f->last;
  }

  for (f = fList->head; f; f = f->next) {
    if (f->last == time) 
      removeFinger(fList, f);
  }
}

// get predecessor and successor of x
void getNeighbors(Node *n, ID x, ID *pred, ID *succ)
{
  FingerList *fList = n->fingerList;
  Finger     *f;

  if (!fList->size) {
    *succ = *pred =  n->id;
    return;
  }

  if (between(x, n->id, fList->head->id, NUM_BITS) || 
      (fList->head->id == x)) {
    *pred = n->id;
    *succ = fList->head->id;
    return;
  }

  for (f = fList->head; f; f = f->next) {
    *pred = f->id;
    if (!f->next) {
      *succ = n->id;
      break;
    } 
    if (between(x, f->id, f->next->id, NUM_BITS) || (f->next->id == x)) {
      *succ = f->next->id;
      break;
    }
  }
}


void printFingerList(Node *n)
{
  Finger *f = n->fingerList->head;

  printf("   finger list:");
  
  for (; f; f = f->next) {
    printf(" %d", f->id);
    if (f->next)
      printf(",");
  }

  if (n->fingerList->size)
    printf(" (h=%d, t=%d, size=%d)", n->fingerList->head->id, 
	    n->fingerList->tail->id, n->fingerList->size); 
    
  printf("\n");
}
