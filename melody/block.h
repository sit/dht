#ifndef _BLOCK_H_
#define _BLOCK_H_
/*
 *
 * Copyright (C) 2002  James Robertson (jsr@mit.edu),
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

#include "sfsmisc.h"
#include "dhash.h"

#define BLOCKSIZE 8192
#define BLOCKPAYLOAD (BLOCKSIZE - (3*sizeof(int)))

/* this is the format of the blocks stored in dhash. this could
   probably be simplified. size is a bit redundant
*/
struct melody_block {
  int type; //2 for venti, 1 for data
  int offset; // for debugging/sanity
  int size; //size of this block
  char data[BLOCKPAYLOAD];
};

/* venti_block is used to keep track of content hashes of other
   blocks. it is meant to be used in a "stack" of venti_blocks, where
   each parent keeps track of the content hashes of the child
   venti_blocks. the top-most venti_blocks keep track of the content
   hashes of the actual data blocks. adding a hash to a venti_block
   may fill it up, and it will then insert itself into dhash, refresh
   itself and tell it's parent about the block that it just stored.
*/

class venti_block {
  dhashclient *dhash;
  struct melody_block data;
  char *hashindex;
  venti_block *parent;
  int offset;
  bool done;
  callback<void, int, bigint>::ptr done_cb;

  bool full();
  void reset(cbv after);
  void reset_cb(cbv after, bool error, chordID key);
  void reset_s();
  void reset_cb_s(bool error, chordID key);
  void close_cb(bool error, chordID key);
  void more_init(venti_block *ap, int dummy);
  void more_init_gb(venti_block *ap, cbv cb, int dummy);

  void get_block_cb(melody_block *bl, cbi cb, int of, ptr<dhash_block> blk);
  void get_block_rc(melody_block *bl, cbi cb, int of, int dummy);
  void get_block2 (melody_block *bl, cbi cb, int of);

 public:
  void add_hash(bigint *hash, cbv after);
  void add_hash_s(bigint *hash);
  bool empty();

  // for creating a vstack for new files
  venti_block(dhashclient *dh, callback<void, int, bigint>::ptr done_cb);

  // for creating a vstack for existing files
  venti_block(dhashclient *dh, melody_block *bl, venti_block *ap);
  venti_block(dhashclient *dh, venti_block *ap, cbv acb);

  // fetch single block. can use sequentially
  void get_block (melody_block *bl, cbi);
  void skip(int blocks, int dummy);

  void close(int size);

  ~venti_block();
};

void null();

#endif
