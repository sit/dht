/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "tmgdmalloc.h"
#include <stdio.h>
#include <assert.h>


// -------------------------------------------------------------------------
//
// To debug the internals of TMG Dmalloc
//
//
struct __TMG_internal {
  const char* file;
  unsigned line;
};

extern hash_map<void*, __TMG_internal*> __tmg_internals;
extern unsigned __tmg_internalallocs;
extern void __tmg_internaldel(void *p);

template<class C>
inline
C*
__tmg_internalnew(C *p, const char* file, unsigned line)
{
  assert(__tmg_internals.find(p) == __tmg_internals.end());
  __TMG_internal *tmgint = new __TMG_internal;
  tmgint->file = file;
  tmgint->line = line;
  __tmg_internals[p] = tmgint;
  return p;
}

void
__tmg_internaldel(void *p)
{
  assert(__tmg_internals.find(p) != __tmg_internals.end());
  __TMG_internal *tmgint = __tmg_internals[p];
  delete tmgint;
  __tmg_internals.erase(p);
}



// # define TMGNEW(X, args...) __tmg_internalnew<X>(new X(args), __FILE__, __LINE__);
// # define TMGDEL(x) (__tmg_internaldel((void*)x), delete x)
# define TMGNEW(X, args...) new X(args)
# define TMGDEL(x) delete x
//
// -------------------------------------------------------------------------
//


struct __TMG_c2info;

// maps typeid class name to a __TMG_filemap
typedef hash_map<unsigned, __TMG_c2info*> __TMG_sizemap;

// maps a line number to a __TMG_sizemap
typedef hash_map<unsigned, unsigned> __TMG_linemap;

// maps a file name to a __TMG_linemap
typedef hash_map<const char*, __TMG_linemap*> __TMG_filemap;

struct __TMG_c2info {
  __TMG_c2info() { allocs = 0; files = TMGNEW(__TMG_filemap); assert(files); }
  ~__TMG_c2info() { assert(!allocs); assert(!files->size()); TMGDEL(files); }
  unsigned allocs;
  __TMG_filemap *files;
};


// maps pointers to all the info one would want about it
struct __TMG_p2info {
  __TMG_p2info(const char *f, unsigned l, size_t s) :
      file(f), line(l), size(s) {}
  const char *file;
  unsigned line;
  size_t size;
};
typedef hash_map<void*, __TMG_p2info*> __TMG_p2infomap;



__TMG_sizemap __tmg_smap;
__TMG_p2infomap __tmg_p2infomap;
unsigned __tmg_totalallocs = 0;

// to debug TMG Dmalloc itself
unsigned __tmg_internalallocs = 0;
hash_map<void*, __TMG_internal*> __tmg_internals;


void
__tmg_dmalloc_new(void *p, size_t size, const char *fname, unsigned lineno)
{
  __TMG_c2info *c2i = 0;
  __TMG_linemap *linemap = 0;

  // find the class entry and update the number of allocs
  if(__tmg_smap.find(size) == __tmg_smap.end()) {
    c2i = TMGNEW(__TMG_c2info);
    assert(c2i);
    __tmg_smap[size] = c2i;
  } else {
    c2i = __tmg_smap[size];
  }
  c2i->allocs++;

  // in correct file/linenumber entry do update
  if(c2i->files->find(fname) == c2i->files->end()) {
    linemap = TMGNEW(__TMG_linemap);
    assert(linemap);
    (*c2i->files)[fname] = linemap;
  } else {
    linemap = (*c2i->files)[fname];
  }
  (*linemap)[lineno]++;

  // now take care of the entry to map pointer to the info
  assert(__tmg_p2infomap.find(p) == __tmg_p2infomap.end());

  __TMG_p2info *p2i = TMGNEW(__TMG_p2info, fname, lineno, size);
  assert(p2i);
  __tmg_p2infomap[p] = p2i;

  __tmg_totalallocs++;
}



void
__tmg_dmalloc_del(void *p)
{
  if(__tmg_p2infomap.find(p) == __tmg_p2infomap.end())
    return;

  // find the information that maps the void pointer to all the
  // required information and delete it from the info map.
  __TMG_p2info *p2i = 0;
  p2i = __tmg_p2infomap[p];
  __tmg_p2infomap.erase(p);

  assert(__tmg_smap.find(p2i->size) != __tmg_smap.end());
  __TMG_c2info *c2i = __tmg_smap[p2i->size];
  c2i->allocs--;

  assert(c2i->files->find(p2i->file) != c2i->files->end());
  __TMG_linemap *linemap = (*c2i->files)[p2i->file];

  assert(linemap->find(p2i->line) != linemap->end());
  if(!(--(*linemap)[p2i->line])) {
    linemap->erase(p2i->line);
  }

  if(!linemap->size()) {
    c2i->files->erase(p2i->file);
    TMGDEL(linemap);
  }

  if(!c2i->allocs) {
    assert(!c2i->files->size());
    __tmg_smap.erase(p2i->size);
    TMGDEL(c2i);
  }

  TMGDEL(p2i);
  __tmg_totalallocs--;
};


void
__tmg_dmalloc_info(void *p)
{
  __TMG_p2info *p2i = 0;
  assert(__tmg_p2infomap.find(p) != __tmg_p2infomap.end());
  p2i = __tmg_p2infomap[p];

  printf("pointer %p, class size %d, file %s, line %d\n",
      p,
      p2i->size,
      p2i->file,
      p2i->line);
}


void
__tmg_dmalloc_stats()
{
#ifndef WITH_TMGDMALLOC
  return;
#else
  // for each class
  printf("TMG Dmalloc Stats\n------------------------------\n");
  printf("Total allocs: %d\n", __tmg_totalallocs);
  for(__TMG_sizemap::const_iterator i = __tmg_smap.begin(); i != __tmg_smap.end(); ++i) {
    printf("size %d, allocated: %d\n", i->first, i->second->allocs);
    for(__TMG_filemap::const_iterator j = i->second->files->begin(); j != i->second->files->end(); j++) {
      for(__TMG_linemap::const_iterator k = j->second->begin(); k != j->second->end(); ++k) {
        printf("  %s:%d %d\n", j->first, k->first, k->second);
      }
    }
  }
  printf("\n");

  // printf("TMG Dmalloc Internal Stats\n------------------------------\n");
  // printf("Total allocs: %d\n", __tmg_internalallocs);
  // for(hash_map<void*, __TMG_internal*>::const_iterator i = __tmg_internals.begin(); i != __tmg_internals.end(); ++i) {
  //   printf("pointer %p, %s:%d\n", i->first, i->second->file, i->second->line);
  // }
  // printf("\n");
#endif
}

void *
operator new(size_t size, const char* file, unsigned line) 
{
  void *p = malloc(size);
  __tmg_dmalloc_new(p, size, file, line);
  return p;
}

void *
operator new[](size_t size, const char* file, unsigned line) 
{
  void *p = malloc(size);
  __tmg_dmalloc_new(p, size, file, line);
  return p;
}


// protects delete and delete[] from being overloaded when
// WITH_TMGDMALLOC is not defined
#ifdef WITH_TMGDMALLOC
void
operator delete(void *p)
{
  __tmg_dmalloc_del(p);
  free(p);
}

void
operator delete[](void *p)
{
  __tmg_dmalloc_del(p);
  free(p);
}
#endif
