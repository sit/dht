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
#include "list.h"
#include "cs_client.h"

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

class melody_file;

class venti_block {
  struct melody_block data;
  char *hashindex;
  venti_block *parent;
  int offset;
  bool done;
  melody_file *conn;
  callback<void, int, bigint>::ptr done_cb;

  bool full();
  bool empty();
  void reset();
  void reset_cb(bool error, chordID key);
  void close_cb(bool error, chordID key);
  void more_init(melody_file *ac, venti_block *ap, int dummy);
  void more_init_gb(melody_file *ac, venti_block *ap, callback<void>::ref, int dummy);

  void get_block_cb(melody_block *bl, cbi cb, int of, ptr<dhash_block> blk);
  void get_block_rc(melody_block *bl, cbi cb, int of, int dummy);
  void get_block2 (melody_block *bl, cbi cb, int of);

 public:
  void add_hash(bigint *hash);

  // for creating a vstack for new files
  venti_block(melody_file *ac, callback<void, int, bigint>::ptr done_cb);

  // for creating a vstack for existing files
  venti_block(melody_file *ac, melody_block *bl);
  venti_block(melody_file *ac, venti_block *ap);
  venti_block(melody_file *ac, venti_block *ap, callback<void>::ref);

  // fetch single block. can use sequentially
  void get_block (melody_block *bl, cbi);
  void skip(int blocks, int dummy);

  void close(int size);
};

/* simple interface for storing and retrieving files in dhash using
the venti_blocks to keep track of the data.  */

class melody_file {
 public:
  melody_file(str csock, callback<void, str>::ptr scb);

  // for storing a file
  void openw(callback<void, int, bigint>::ptr done_cb, callback<void>::ptr error_cb);
  void write(const char *buf, int len); // sequential, one-time write (can't update existing file)
  // for fetching a file
  void openr(bigint filehash, callback<void, const char *, int, int>::ptr read_cb, cbi ready_cb);
  void skip(int blocks);
  void next();
  void close();

  bool sleeptest(cs_client *c);
  void sleepdied(cs_client *c);
  dhashclient *dhash;
  int blocks;
  callback<void, str>::ptr statuscb;

 private:
  int size, wsize, venti_depth, outstanding;
  melody_block cbuf;
  venti_block *vstack;
  tailq < cs_client, &cs_client::sleep_link > sleeping;
  callback<void, const char *, int, int>::ptr read_cb;
  suio wbuf;
  callback<void>::ptr error_cb;

  void find_venti_depth(int asize);
  void venti_cb(cbi ready_cb, ptr<dhash_block> blk);
  void next_venti_cb(int index, cbi ready_cb);
  void write_cb (bool error, chordID key);
  void next_cb(int offset);
  void flush();
};

#endif
