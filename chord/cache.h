#ifndef _CACHE_H_
#define _CACHE_H_

template<class KEY, class VALUE, class R = qhash_lookup_return<VALUE> >
  // R: Kludge so we don't return pointers to refcounted objects
class myvs_cache {
  struct cache_entry {
    myvs_cache *const c;
    const KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (myvs_cache<KEY, VALUE> *cc, const KEY &kk, const VALUE &vv)
      : c (cc), k (kk), v (vv)
    {      
      c->lrulist.insert_tail (this);
      c->entries.insert (this);
      c->num_cache_entries++;
      while (c->num_cache_entries > 
	     implicit_cast<u_int> (c->max_cache_entries)) {
        if (c->fcb) (c->fcb) (c->lrulist.first->k, c->lrulist.first->v);
        delete c->lrulist.first;
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

  typedef callback<void, KEY, VALUE>::ptr cachecb_t;
  
 private:
  friend class cache_entry;
  ihash<const KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;
  u_int max_cache_entries;
  cachecb_t fcb;
 public:
  myvs_cache (u_int max_entries = 250) : num_cache_entries (0), 
    max_cache_entries (max_entries), 
    fcb (NULL) { };

  ~myvs_cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  
  void set_flushcb (cachecb_t cb) {
    fcb = cb;
  }

  bool insert (const KEY& kk, const VALUE &vv)
  {
    cache_entry *ad = entries[kk];
    if (ad) 
      return false;
    
    New cache_entry (this, kk, vv);
    return true;
  }

  bool remove (const KEY& kk)
  {
    cache_entry *ad = entries[kk];
    if (!ad)
      return false;

    delete ad;
    return true;
  }
  
  void traverse (cachecb_t cb)
  {
    cache_entry *e = entries.first ();
    while (e) 
    {
      cb (e->k, e->v);
      e = entries.next (e);
    }
  }

  typename R::type operator[] (const KEY& kk) {
    cache_entry *ad = entries[kk];
    if (ad) {
      ad->touch ();
      return R::ret (&ad->v);
    } else
      return R::ret (NULL);
  }
};

#endif _CACHE_H_





