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

#include "retrieve_manager.h"
#include <dhash_common.h>
#include <dhashclient.h>

void
retrieve_manager::retrieve (bigint id, callback<void, ptr<dhash_block> >::ptr cb)
{
  //  warn << "retrieve_manager::requested\n";
  retrieve_block *tmp = New retrieve_block(cb);
  blocks.insert_tail(tmp);
  b_count++;

  dhash->retrieve(id, DHASH_CONTENTHASH, wrap(mkref(this), &retrieve_manager::got_block, tmp));
}

void
retrieve_manager::got_block (retrieve_block *tmp, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> p)
{
  //  warn << "retrieve_manager::retrieved\n";
  if(!blk) {
    warn << "retrieve_manager::got_block no blk\n";
    tmp->cb(blk);
    return;
  }

  tmp->blk = blk;

  while(blocks.first && blocks.first->blk) {
    blocks.first->cb(blocks.first->blk);
    tmp = blocks.first;
    blocks.remove(tmp);
    b_count--;
    delete tmp;
  }
}
