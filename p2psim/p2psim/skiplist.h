#ifndef _SKIPLIST_H_
#define _SKIPLIST_H_

#ifdef DMALLOC
#include <typeinfo>
#include <dmalloc.h>
#endif /* DMALLOC */

#include <assert.h>
#include <unistd.h>
#include "keyfunc.h"

/*
 * Template implementation of Skip Lists.
 * Additional methods for successors/predecessors with wrapping
 *
 *  William Pugh. Skip Lists: Skip lists: A probabilistic alternative
 *  to balanced trees.  Communications of the ACM, 33(6):668--676,
 *  June 1990.
 */

#define SKLIST_MAX_LEVS 2

template<class T>
struct sklist_entry {
  T *previous;
  T *forward[SKLIST_MAX_LEVS]; // xxx save memory here?
};

template<class T, class K, K T::*key, sklist_entry<T> T::*field,
  class C = compare<K> >
class skiplist {
  const C cmp;
  T *head;
  T *tail;
  unsigned int sz;

  // Number of distinct levels. Highest valid lvl in any of the forward
  // pts is forward[lvl - 1].
  unsigned int lvl;

  /* No copying */
  skiplist (const skiplist &);
  skiplist &operator = (const skiplist &);

  static unsigned int rndlvl () {
    unsigned int l = 1;
    while (((random () / (float) RAND_MAX) < 0.50) &&
	   l < SKLIST_MAX_LEVS)
      l++;
    return l;
  }
  
 protected:
  void insert_head (T *elm) {
    // requires: T not be in list already.
    unsigned int newlvl = rndlvl (); // new level for old head
    unsigned int i;

    // initialize pointers of this element. as head, it will
    // be an element of height = to max level in list. old head will
    // be cut (or grown) to newlvl. new head will be at max (lvl, newlvl).
    (elm->*field).previous = NULL;
    if (newlvl < lvl) {
      assert (head);
      // old head is now shorter
      for (i = 0; i < newlvl; i++)
	(elm->*field).forward[i] = head;
      for (i = newlvl; i < lvl; i++) {
	(elm->*field).forward[i] = (head->*field).forward[i];
	(head->*field).forward[i] = NULL;
      }
    } else {
      // old head is now taller.
      for (i = 0; i < newlvl; i++)
	(elm->*field).forward[i] = head;
      if (head)
	for (i = lvl; i < newlvl; i++)
	  (head->*field).forward[i] = NULL;
      lvl = newlvl;
    }
    if (head)
      (head->*field).previous = elm;
    if (!tail)
      tail = elm;
    
    head = elm;
    for (i = lvl; i < SKLIST_MAX_LEVS; i++)
      (head->*field).forward[i] = NULL;
  }

 public:
  skiplist () : head (NULL), tail (NULL), sz (0), lvl (1) {}

  // If list is empty, return NULL.
  // If list has more than one:
  //   return first element x such that x->next.key >= k
  //   if the key is before the head, return the last element.
  T *closestpred (const K &k) const {
    T *x = head;
    if (x == NULL)
      return NULL;
    if (cmp (k, x->*key) <= 0)
      return tail;
    
    for (int i = lvl - 1; i >= 0; i--) {
      while ((x->*field).forward[i] &&
	     cmp ((x->*field).forward[i]->*key, k) < 0)
	x = (x->*field).forward[i];
    }
    return x;
  }

  T *search (const K &k) const {
    if (head == NULL)
      return NULL;
    if (cmp (k, head->*key) < 0)
      return NULL;
    if (cmp (k, head->*key) == 0)
      return head;

    T *x = closestpred (k);
    if (x == NULL)
      return NULL;
    x = (x->*field).forward[0];
    if (x && cmp (x->*key, k) == 0)
      return x;
    
    return NULL;
  }

  // If the list is empty, return NULL
  // If the list has more than one:
  //   return first element x such that x->key >= k
  //   if it happens that the key is after the last element
  //   in the list, return the first element. (?)
  T *closestsucc (const K &k) const {
    if (head == NULL)
      return NULL;
    if (cmp (k, head->*key) <= 0)
      return head;
    if (cmp (k, tail->*key) > 0)
      return head;

    T *x = closestpred (k);
    if (x == NULL)
      return NULL;
    if (x == tail)
      x = head;
    else
      x = (x->*field).forward[0];

    return x;
  }

  bool insert (T *elm) {
    if (head == NULL) {
      sz++;
      insert_head (elm);
      return true;
    } else {
      int headcmp = cmp (elm->*key, head->*key);
      if (headcmp < 0) {
	sz++;
	insert_head (elm);
	return true;
      } else if (headcmp == 0) {
	return false;
      }
    }
    
    T *update[SKLIST_MAX_LEVS];
    T *x = head;
    T *prev = NULL;
    for (int i = lvl - 1; i >= 0; i--) {
      while ((x->*field).forward[i] &&
	     cmp ((x->*field).forward[i]->*key, elm->*key) < 0)
	x = (x->*field).forward[i];
      update[i] = x;
    }
    prev = x;
    x = (x->*field).forward[0];
    if ((x == NULL) || cmp (x->*key, elm->*key) > 0) {
      unsigned int newlvl = rndlvl ();
      unsigned int i;
      if (newlvl > lvl) {
	for (i = lvl; i < newlvl; i++)
	  update[i] = head;
	lvl = newlvl;
      }
      for (i = 0; i < newlvl; i++) {
	(elm->*field).forward[i] = (update[i]->*field).forward[i];
	(update[i]->*field).forward[i] = elm;
      }
      for (i = newlvl; i < SKLIST_MAX_LEVS; i++)
	(elm->*field).forward[i] = NULL;
      
      if (x == NULL)
	tail = elm;
      else
	(x->*field).previous = elm;
      (elm->*field).previous = prev;
    } else {
      return false; // no dupes
    }
    sz++;
    return true;
  }

  T *remove (const K &k) {
    T *oldhead = head;
    if (head == NULL) {
      return NULL;
    }
    if (cmp (head->*key, k) == 0) {
      T *next = (head->*field).forward[0];
      sz--;
      if (next == NULL) {
	head = NULL;
	tail = NULL;
	lvl = 1;
	return oldhead;
      } else {
	unsigned int i;
	for (i = lvl - 1; i >= 0; i--) {
	  if ((head->*field).forward[i] != next)
	    (next->*field).forward[i] = (head->*field).forward[i];
	  else
	    break;
	}
	(next->*field).previous = NULL;
	head = next;
	while (lvl > 1 && (head->*field).forward[lvl - 1] == NULL)
	  lvl--;

	return oldhead;
      }
    }
    
    T *update[SKLIST_MAX_LEVS];
    T *prev, *next;
    T *x = head;
    for (int i = lvl - 1; i >= 0; i--) {
      while ((x->*field).forward[i] &&
	     cmp ((x->*field).forward[i]->*key, k) < 0)
	x = (x->*field).forward[i];
      update[i] = x;
    }
    prev = x;
    x = (x->*field).forward[0];
    if (x && (cmp (x->*key, k) == 0)) {
      unsigned int i;
      next = (x->*field).forward[0];
      for (i = 0; i < lvl; i++) {
	if ((update[i]->*field).forward[i] != x)
	  break;
	(update[i]->*field).forward[i] = (x->*field).forward[i];
      }
      while (lvl > 1 && (head->*field).forward[lvl - 1] == NULL)
	lvl--;
      if (next)
	(next->*field).previous = prev;
      else
	tail = prev;
      sz--;
      return x;
    } else {
      return NULL;
    }
  }

  T *first () const {
    return head;
  }

  T *last () const {
    return tail;
  }

  unsigned int size () const {
    return sz;
  }
  
  static T *next (T *elm) {
    return (elm->*field).forward[0];
  }

  static T *prev (T *elm) {
    return (elm->*field).previous;
  }

#ifdef _CALLBACK_H_INCLUDED_
  void traverse (typename callback<void, T *>::ref cb) const {
    T *p, *np;
    for (p = head; p; p = np) {
      np = (p->*field).forward[0];
      (*cb) (p);
    }
  }
  void rtraverse (typename callback<void, T *>::ref cb) const {
    T *p, *np;
    for (p = tail; p; p = np) {
      np = (p->*field).previous;
      (*cb) (p);
    }
  }
#endif /* _CALLBACK_H_INCLUDED_ */  

  bool repok () const {
    if (head == NULL) {
      assert (lvl == 1);
      if (lvl != 1) return false;
      assert (tail == NULL);
      if (tail != NULL) return false;
      return true;
    }

    T *p, *np;

    p = head;
    assert ((p->*field).previous == NULL);
    
    // All elements are unique and increasing.
    for (int i = lvl - 1; i >= 0; i--) {
      while (p != NULL) {
	np = (p->*field).forward[i];
	if (np && cmp (p->*key, np->*key) >= 0) {
	  assert (false);
	  return false;
	}
	if (i == 0 && np && (np->*field).previous != p) {
	  assert (false);
	  return false;
	}
	// The end is right.
	if (i == 0 && !np) {
	  assert (p == tail);
	  if (p != tail) return false;
	}
	p = np;
      }
    }
    
    return true;
  }
};

#endif /* _SKIPLIST_H_ */
