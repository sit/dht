/*
 *
 * Copyright (C) 2001  Josh Cates (cates@mit.edu),
 *                     Frank Dabek (fdabek@lcs.mit.edu), 
 *   		       Massachusetts Institute of Technology
 * 
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

#ifndef __LRUCACHE_H__
#define __LRUCACHE_H__ 1

#include "qhash.h"

//
// NB. perhaps should build the lrucache ontop of an ilrucache.
//


template<class K, class V, class H = hashfn<K>, class E = equals<K>,
  // XXX - We need to kludge this for both g++ and KCC
  class R = qhash_lookup_return<V>,
  ihash_entry<qhash_slot<K, V> > qhash_slot<K,V>::*kludge
	= &qhash_slot<K,V>::link>
class lrucache {
  private:
  u_int maxsize;
  const H hash;
  
  struct lrucache_entry {
    K  k;
    V  v;
    ihash_entry<lrucache_entry> hashlink;
    tailq_entry<lrucache_entry> lrulink;
    lrucache_entry (const K &k,  const V &v) : k (k), v (v) {}
  };
  ihash<K, lrucache_entry, &lrucache_entry::k, &lrucache_entry::hashlink, H> entries;
  tailq<lrucache_entry, &lrucache_entry::lrulink> lrulist;

  public:
  lrucache (u_int max = 0) : maxsize (max) {}

  size_t size () const { return entries.size(); }

  void insert (const K &k, CREF (V) v) {
    if (maxsize && size() == maxsize)
      remove_oldest();
    lrucache_entry *e = New lrucache_entry(k, v);
    entries.insert (e);
    lrulist.insert_tail(e);
  }

  void insert (const K &k, NCREF (V) v) {
    if (max_size && size() == max_size)
      remove_oldest();
    lrucache_entry *e = New lrucache_entry(k, v);
    entries.insert (e);
    lrulist.insert_tail(e);
  }

  void remove (const K &k) {
    lrucache_entry *e = entries[k];
    if (e) {
      lrulist.remove(e);
      entries.remove(k);
      delete e;
    }
  }

  typename R::type operator[] (const K &k) {
    lrucache_entry *e = entries[k];
    if (e) {
      lrulist.remove(e);
      lrulist.insert_tail(e);
      return R::ret (&entries[k]->v);
    }
    return R::ret (NULL);
  }

  typename R::type remove_oldest () {
    lrucache_entry *e = lrulist.first;
    if (e) {
      lrulist.remove(e);
      entries.remove(e);
      return R::ret (&e->v);
    }
    return R::ret (NULL);
  }

  //
  // add functions for in lru order traversal
  //
};

#endif // __LRUCACHE_H__

