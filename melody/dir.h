#ifndef _DIR_H_
#define _DIR_H_
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

#include "async.h"
#include "block.h"
#include "dhash.h"
class cs_client;

#define DIRROOT { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6 }

struct dir_record {
  int type;
  char key[20];
  int size;
  char entry[256];
};

class dir {
  melody_file *cc;
  bigint dirhash;
  suio buf;
  unsigned int entry_index;
  int strip;
  bool exist;
  callback<void, const char *, int, int>::ptr fileout;
  callback<void, int>::ptr filehead;
  callback<void>::ptr gotdir;
  // FIXME initialize all?

  void opendir(vec<str> pathelm, bigint tmp);
  void flush_cb(bool done, cbv redir, bool error, chordID key);
  void find_entry(vec<str> pathelm, ptr<dhash_block> blk);
  void found_entry(ptr<dhash_block> blk);
  void root_test_cb(ptr<dhash_block> blk);
  void add(str name, int type, bigint tmphash, int size, cbv redir);
  void add2(int type, bigint tmphash, int size, cbv redir);
  void null();

public:
  cs_client *cs;

  ~dir();
  dir(melody_file *acc, callback<void, const char *, int, int>::ptr afileout, callback<void, int>::ptr afilehead, cs_client *cs);
  void add_dir(str dir, str parent, cbv redir);
  void add_file(str dir, str parent, bigint filehash, int size, cbv redir);
  void opendir(str path, callback<void>::ptr agotdir);
  bool more(void);
  void readdir(struct dir_record *dr);
  bool exists() { return exist; }
  void root_test();
};

#endif
