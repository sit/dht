/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

#include "incl.h"

void testEviction(FingerList *fList, ID id);
void evictFinger(FingerList *fList, ID id);
void cleanFingerList(FingerList *fList);
void setPresent(ID id);


Finger *newFinger(ID id)
{
  Finger *finger;

  // allocate space for new finger
  if ((finger = (Finger *)calloc(1, sizeof(Finger))) == NULL)
    panic("newFinger: memory allocation error!\n");
    
  finger->id     = id;
  finger->last   = Clock;
  finger->expire = MAX_TIME;

  return finger;
}

// return closest node that succeeds n from n's fingerList.
// Note: in general, this node is the n's successor on the 
// circle; the exceptions are when n is in the process
// of joining the system, or when the known successor fails 
ID getSuccessor(Node *n)
{
  if (n->fingerList->head)
    return n->fingerList->head->id;
  else
    return n->id;
}

// return closest node that preceeds n from n's fingerList
ID getPredecessor(Node *n)
{
  if (n->fingerList->head)
    return n->fingerList->tail->id;
  else
    return n->id;
}

void loseFinger(Node* n, ID* remove)
{
    FingerList* fList;
    Finger* f;

    // something happened between the time the finger was absent and now.
    // In real life, this means nothing would happen, since the timeout
    // couldn't be detected until this event fired. In simulation world, it
    // means we shouldn't be evicting the finger. The sequence of events
    // that would have generated this is :
    //    life as usual | 'remove' dies ... | 'insertFinger' called ...
    //       (200 ms go by, say) | 'remove' comes back |  500 ms
    //       have gone by since 'insertFinger' was called | loseFinger is
    //       called
    if (getNode(*remove)->status == PRESENT) {
	free(remove);
	return;
    }

    fList = n->fingerList;
    if (fList && (f = getFinger(fList, *remove)))
	removeFinger(fList, f);

    free(remove);
}
 


// insert new finger in the list; if the finger is already in the
// list, refresh it
void insertFinger(Node *n, ID id)
{
  FingerList *fList = n->fingerList;
  Finger *f, *ftemp;

  if (n->id == id)
    return;

  updateDocList(n, getNode(id));
  updateDocList(getNode(id), n);

#ifdef OPTIMIZATION
  // optimization: if n's predecessor changes 
  // announce n's previous predecessor
  if (fList && between(id, getPredecessor(n), n->id)) {
    Request *r;
    ID       pred = getPredecessor(n);

    r = newRequest(id, REQ_TYPE_SETSUCC, REQ_STYLE_ITERATIVE, n->id);
    genEvent(pred, insertRequest, (void *)r, Clock + intExp(AVG_PKT_DELAY));
  }     
#endif // OPTIMIZATION

  if ((f = getFinger(fList, id))) {
    // just refresh finger f
    f->last   = Clock;
    f->expire = MAX_TIME;
    return;
  }

  if (!fList->size) {
    // fList empty; insert finger in fList
    fList->size++;
    fList->head = fList->tail = newFinger(id);

    testEviction(fList, n->id);

    setPresent(id);
    return;
  }

  if (between(id, n->id, fList->head->id)) {
    ftemp = fList->head;
    fList->head = newFinger(id);
    fList->head->next = ftemp;
    fList->size++;
    
    testEviction(fList, n->id);
    setPresent(id);
    return;
  }

  for (f = fList->head; f; f = f->next) {
    if (!f->next) {
      fList->tail = f->next = newFinger(id);
      fList->size++;
      break;
    }

    if (between(id, f->id, f->next->id)) {
      // insert new finger just after f
      ftemp = f->next;
      f->next = newFinger(id);
      f->next->next = ftemp;
      fList->size++;
      break;
    }
  }

  // if we need to evict, do it after the latest has been added, the
  // reason being that the latest addition may be the only candidate
  // for eviction (if, for example, the elements in the fingertable
  // were only the successors and the fingers, neither of which should
  // be removed).
  testEviction(fList, n->id);
  return;
}

void
testEviction(FingerList* fList, ID owner)
{
  if (fList->size > max_location_cache) {

    // note that b/c the first successor is also the first
    // finger, we have that in certain cases an fList size of
    // num_succs + num_fingers is too big (if the user set
    // num_successors to 1 and max_location_cache to 24, e.g.)
    assert(fList->size >= num_successors + num_fingers); 

    // we are assured of being able to evict an element since
    // the list is big enough
    evictFinger(fList, owner);
    assert(fList->size == max_location_cache);
  }

}

// search and return finger id from fList
Finger *getFinger(FingerList *fList, ID id)
{
  Finger *f;

  for (f = fList->head; f; f = f->next)
    if (f->id == id)
      return f;
  
  return NULL;
}


// set the state of the node to "PRESENT".
// a node n is PRESENT only after another node (i.e., n's 
// predecessor) points to it; Only PRESENT nodes are part 
// of the network.
// when n becomes PRESENT the documents whose identifiers
// fall between itself and its predecessor are transferred 
// to n   
void setPresent(ID id)
{
  Node *n = getNode(id);

  if (n && n->status == TO_JOIN) {
    Node *s = getNode(getSuccessor(n));
    n->status = PRESENT;
    copySuccessorFingers(n);
    updateDocList(n, s);
    printf("node %d joins at time %f\n", n->id, Clock); 
  }    
}

// remove finger f from the finger list fList
void removeFinger(FingerList *fList, Finger *f)
{
  Finger *f1;

  if (!fList->head)
    return;

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
  Finger *f;

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

// evict the location cache entry that has not been refrerred for the 
// longest time (in LRU fashion). the successors are effectively
// pinned in. also, this function computes, in an online fashion,
// which of the elements are bona fide fingers (as opposed to 
// location cache entries), and the ones that are actually fingers
// are not considered for LRU replacement. this models what the real Chord
// algorithm does. --MW
void evictFinger(FingerList *fList, ID owner)
{
  Finger *f = NULL, *ftemp = NULL, *fmarked = NULL;
  ID previd;
  int     i;
  double time = MAX_TIME;
  int k = 0;	// current finger, 
  int is_finger = 0;

  ftemp = fList->head;

  // don't evict successors
  for (i = 0; i < num_successors; i++)
    ftemp = ftemp->next;

  // should be at least as many location cache entries as 
  // successors and as fingers, so we shouldn't be out of entries
  // here
  if (!ftemp)
      assert(0); 

  previd = owner;

  for (f = ftemp; f; f = f->next) {
    
     is_finger = 0;
     // rounding up b/c that's consistent with our model if storing
     // quantities at their successors
     ID succ_id = successorId(owner, 1 << k);

     // need to avoid evicting fingers.  this while loop skips over fingers
     // that count as multiple order fingers (which is inevitable when N
     // is much less than 2^m, where m is the number of bits for ID space.)
     // On the first iteration, previd = owner, so we're figuring out
     // how many fingers were covered by the successor list.
    while ((between(succ_id, previd, f->id ) || f->id == succ_id) 
	     && k < num_fingers) { 
	 is_finger = 1; 
	 k++; 
	 succ_id = successorId(owner, 1 << k);
     }

     // checks for LRU; only examines non-fingers
     if (!is_finger && f->last < time) {
	time = f->last;
	fmarked = f;
     }
    
     previd = f->id;
  }

  assert(fmarked);
  removeFinger(fList, fmarked);
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

  if (between(x, n->id, fList->head->id) || 
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
    if (between(x, f->id, f->next->id) || (f->next->id == x)) {
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

