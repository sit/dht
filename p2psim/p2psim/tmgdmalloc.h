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

#ifndef __TMG_DMALLOC_H
#define __TMG_DMALLOC_H

//
// Usage:
//
// ./configure --with-tmgdmalloc
// #include "tmgdmalloc.h"
//
// Then use New rather than new.
//
// __tmg_dmalloc_stats will tell you the location of un-delete-d objects.
//

#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
# include <ext/hash_map>
using namespace __gnu_cxx;
namespace __gnu_cxx {
#else
# include <hash_map>
using namespace std;
namespace std {
#endif
  template<> struct hash<void*> {
    size_t operator()(void *x) const {
      return (size_t) x;
    }
  };
}

void __tmg_dmalloc_new(void *, size_t, const char*, unsigned);
void __tmg_dmalloc_del(void *);
void __tmg_dmalloc_stats();
void __tmg_dmalloc_info(void *p);

#ifdef WITH_TMGDMALLOC
void* operator new(size_t, const char*, unsigned);
void* operator new[](size_t, const char*, unsigned);
void  operator delete(void *p);
void  operator delete[](void *p);
# define New new(__FILE__, __LINE__)
#else
# define New new
#endif // WITH_TMGDMALLOC

#endif //  __TMG_DMALLOC_H
