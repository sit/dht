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
#include "file.h"
#include "dhash.h"
class cs_client;
#include <sys/time.h>

#define DIRROOT { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6 }

struct dir_record {
  int type;
  char key[20];
  int size;
  struct timeval ctime;
  char entry[256];
};

class dir {
  ptr<melody_file>cc;

  bigint dirhash, vhash;
  suio buf, cbuf;
  unsigned int entry_index;
  int strip;
  bool exist, noread;
  callback<void, const char *, int, int>::ptr fileout;
  callback<void, int, str>::ptr filehead;
  callback<void>::ptr gotdir;
  strbuf combdir;
  vec<str> pathelm;
  // FIXME initialize all?

  void opendir(bigint tmp);
  void opendir_got_venti(cbretrieve_t cbr, ptr<dhash_block> blk);
  void opendir_got_venti_noread(ptr<dhash_block> blk);
  void next_dirblk(cbretrieve_t cbr);
  void find_entry(ptr<dhash_block> blk);
  void found_entry(ptr<dhash_block> blk);
  void root_test_got_rb(ptr<dhash_block> blk);
  void create_venti(cbs redir, str parent, bool error, chordID key);
  void create_venti_done(cbs redir, str parent, bool error, chordID key);
  void root_done(str foo);
  void add(str name, int type, bigint tmphash, int size, cbs redir, str parent);
  void add2(bigint tmphash, int size, cbs redir, str parent);
  void flush_cb(cbs redir, str parent, bool error, chordID key);
  void after_new_dir_block(cbs redir, str parent, bool error, chordID key);
  void after_appended_new_dirhash(cbs redir, str parent, bool error, chordID key);

public:
  cs_client *cs;

  ~dir();
  dir(ptr<melody_file>acc, callback<void, const char *, int, int>::ptr afileout, callback<void, int, str>::ptr afilehead, cs_client *cs);
  void add_dir(str dir, str parent, cbs redir);
  void add_file(str dir, str parent, bigint filehash, int size, cbs redir);
  void opendir(str path, callback<void>::ptr agotdir, bool anoread);
  bool more(void);
  void readdir(struct dir_record *dr);
  bool exists() { return exist; }
  void root_test();
};

#endif
