#ifndef _FILE_H_
#define _FILE_H_
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
#include "block.h"

/* simple interface for storing and retrieving files in dhash using
the venti_blocks to keep track of the data.  */

class melody_file : public virtual refcount {
 public:
  melody_file(str csock, callback<void, str>::ptr scb);
  ~melody_file();

  // for storing a file
  void openw(callback<void, int, bigint>::ptr done_cb, callback<void>::ptr error_cb);
  void write(const char *buf, int len); // sequential, one-time write (can't update existing file)
  // for fetching a file
  void openr(bigint filehash, callback<void, const char *, int, int>::ptr read_cb, callback<void, int, str>::ref ready_cb, str filename);
  void skip(int blocks);
  void next();
  void close();
  void readstop();

  bool sleeptest(cs_client *c);
  void sleepdied(cs_client *c);
  dhashclient *dhash;
  int blocks;
  callback<void, str>::ptr statuscb;
  bool readgo;

 private:
  int size, wsize, venti_depth, outstanding;
  melody_block cbuf;
  venti_block *vstack;
  tailq < cs_client, &cs_client::sleep_link2 > sleeping;
  callback<void, const char *, int, int>::ptr read_cb;
  suio wbuf;
  callback<void>::ptr error_cb;

  void find_venti_depth(int asize);
  void venti_cb(callback<void, int, str>::ref ready_cb, str filename, ptr<dhash_block> blk);
  void next_venti_cb(int index, callback<void, int, str>::ref ready_cb, str filename);
  void write_cb (bool error, chordID key);
  void next_cb(int offset);
  void flush();
};

#endif
