
template<class KEY, class VALUE>
class vs_cache {
  struct cache_entry {
    vs_cache    *c;
     KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (vs_cache<KEY, VALUE> *cc,
	        KEY &kk,  VALUE *vv)
      : c (cc), k (kk)
    {      v = *vv;
    c->lrulist.insert_tail (this);
    c->entries.insert (this);
    c->num_cache_entries++;
    while (c->num_cache_entries > implicit_cast<u_int> (c->max_cache_entries)) {
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
  void enter ( KEY& kk,  VALUE *vv)
    {
      cache_entry *ad = entries[kk];
      if (!ad)
	vNew cache_entry (this, kk, vv);
      else 
	ad->touch ();
    }
  
  void remove (KEY& k) 
    {      
      entries.remove(entries[k]);
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
       warn << "Will traverse " << num_cache_entries << "\n";
       cache_entry *e = entries.first ();
       while (e) 
	 {
	   cb (e->k);
	   e = entries.next (e);
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

