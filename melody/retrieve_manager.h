#ifndef RETRIEVE_MANAGER_H
#define RETRIEVE_MANAGER_H
/*
 *
 * Copyright (C) 2003  James Robertson (jsr@mit.edu),
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

#include <callback.h>
#include <refcnt.h>
#include <list.h>

#include <dhash_prot.h>
#include <chord.h>

class bigint;
class dhash_block;
class dhashclient;

class retrieve_block {
 public:
  callback<void, ptr<dhash_block> >::ptr cb;
  ptr<dhash_block> blk;
  tailq_entry<retrieve_block> te;

  retrieve_block(callback<void, ptr<dhash_block> >::ptr acb) : 
    cb(acb), blk(NULL) {};
};

class retrieve_manager : public virtual refcount {
  dhashclient *dhash;
  tailq<retrieve_block, &retrieve_block::te> blocks;

 public:
  int b_count;

  retrieve_manager (dhashclient *d) : dhash(d), b_count(0) {};
  void retrieve (bigint id, callback<void, ptr<dhash_block> >::ptr cb);
  void got_block (retrieve_block *tmp, dhash_stat s, ptr<dhash_block> blk, vec<chordID> p);
};

#endif
