#include "tmgdmalloc.h"


__TMG_classmap __tmg_cmap;
__TMG_p2infomap __tmg_p2infomap;
unsigned __tmg_totalallocs;


void
__tmg_dmalloc_del(void *p)
{
  __TMG_p2info *p2i = 0;
  assert(__tmg_p2infomap.find(p) != __tmg_p2infomap.end());
  p2i = __tmg_p2infomap[p];
  __tmg_p2infomap.erase(p);

  assert(__tmg_cmap.find(p2i->cname) != __tmg_cmap.end());
  __TMG_c2info *c2i = __tmg_cmap[p2i->cname];
  c2i->allocs--;

  assert(c2i->files->find(p2i->file) != c2i->files->end());
  __TMG_linemap *linemap = (*c2i->files)[p2i->file];

  assert(linemap->find(p2i->line) != linemap->end());
  if(!(--(*linemap)[p2i->line]))
    linemap->erase(p2i->line);

  if(!linemap->size())
    c2i->files->erase(p2i->file);

  if(!c2i->allocs) {
    assert(!c2i->files->size());
    __tmg_cmap.erase(p2i->cname);
    delete c2i;
  }
  __tmg_totalallocs--;
};


void
__tmg_dmalloc_info(void *p)
{
  __TMG_p2info *p2i = 0;
  assert(__tmg_p2infomap.find(p) != __tmg_p2infomap.end());
  p2i = __tmg_p2infomap[p];

  printf("pointer %p, class %s, file %s, line %d\n",
      p,
      p2i->cname.c_str(),
      p2i->file.c_str(),
      p2i->line);
}


void
__tmg_dmalloc_stats()
{
  // for each class
  printf("TMG Dmalloc Stats\n------------------------------\n");
  printf("Total allocs: %d\n", __tmg_totalallocs);
  for(__TMG_classmap::const_iterator i = __tmg_cmap.begin(); i != __tmg_cmap.end(); ++i) {
    printf("class %s, allocated: %d\n", i->first.c_str(), i->second->allocs);
    for(__TMG_filemap::const_iterator j = i->second->files->begin(); j != i->second->files->end(); j++) {
      for(__TMG_linemap::const_iterator k = j->second->begin(); k != j->second->end(); ++k) {
        printf("  %s:%d %d\n", j->first.c_str(), k->first, k->second);
      }
    }
  }
  printf("\n");
}
