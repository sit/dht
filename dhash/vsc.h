/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
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

template<class KEY, class VALUE>
class vs_cache {
  struct cache_entry {
    vs_cache    *c;
    KEY          k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (vs_cache<KEY, VALUE> *cc,
	        KEY &kk,  VALUE *vv)
      : c (cc), k (kk), v (*vv)
    {
      c->lrulist.insert_tail (this);
      c->entries.insert (this);
      c->num_cache_entries++;
      if (c->num_cache_entries > c->max_cache_entries) {
	cache_entry *ad = c->lrulist.first;
	if (c->fcb)
	  (c->fcb) (ad->k, ad->v);
	delete ad;
      }
    }

    ~cache_entry ()
    {
      c->lrulist.remove (this);
      c->entries.remove (this);
      c->num_cache_entries--;
    }

    void touch ()
    {
      c->lrulist.remove (this);
      c->lrulist.insert_tail (this);
    }
  };

  typedef callback<void, KEY, VALUE>::ptr flushcb_t;
  
private:
  friend class cache_entry;   //XXX hashid is a hack that ruins the generic nature of the cache
  ihash<KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink, hashID> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;
  u_int max_cache_entries;
  flushcb_t fcb;
public:
  vs_cache (u_int max_entries = 1000) : num_cache_entries (0), 
    max_cache_entries (max_entries), 
    fcb (NULL) { };

  ~vs_cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  int enter ( KEY& kk,  VALUE *vv)
    {
      cache_entry *ad = entries[kk];
      if (!ad) {
	vNew cache_entry (this, kk, vv);
        return 1;
      } else {
	// XXX What if entering a new value under an old key.  Does
	// touch() really give the expected behavior?  --josh
	ad->touch ();
        return 0;
      }
    }
  
  void remove (KEY& k) 
    {      
      cache_entry *ad = entries[k];
      assert (ad);
      delete ad;
    }

   VALUE *lookup (KEY& kk)
    {
      cache_entry *ad = entries[kk];
      if (ad) {
	ad->touch ();
	return &ad->v;
      }
      return NULL;
    }
  
   void traverse (callback<void, KEY>::ref cb ) 
     {
       cache_entry *e = entries.first ();
       while (e) 
	 {
	   cache_entry *next = entries.next (e); // in case cb deletes e
	   cb (e->k);
	   e = next;
	 }
     }

   VALUE *peek (KEY& k) {
    cache_entry *ad = entries[k];
    if (ad) {
      return &ad->v;
    }
    return NULL;
   }
  
   void set_flushcb (flushcb_t cb ) {
    fcb = cb;
  }

  u_int 
  size () 
  {
    return num_cache_entries;
  }
};

