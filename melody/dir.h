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
  bool exist, noread, error;
  callback<void, const char *, int, int>::ptr fileout;
  callback<void, int, str>::ptr filehead;
  callback<void>::ptr gotdir;
  strbuf combdir;
  vec<str> pathelm;
  // FIXME initialize all?

  void opendir(bigint tmp);
  void opendir_got_venti(cb_ret cbr, 
			 dhash_stat stat,
			 ptr<dhash_block> blk,
			 route p);
  void opendir_got_venti_noread(dhash_stat s, ptr<dhash_block> blk, route p);
  void next_dirblk(cb_ret cbr);
  void find_entry(dhash_stat stat, ptr<dhash_block> blk, route p);
  void found_entry(dhash_stat stat, ptr<dhash_block> blk, route p);
  void root_test_got_rb(dhash_stat stat, ptr<dhash_block> blk, route p);
  void create_venti(cbs redir, str parent, dhash_stat status,
                    ptr<insert_info> i);
  void create_venti_done(cbs redir, str parent, dhash_stat status, 
			 ptr<insert_info> i);
  void root_done(str foo);
  void add(str name, int type, bigint tmphash, int size, cbs redir, str parent);
  void add2(bigint tmphash, int size, cbs redir, str parent);
  void flush_cb(cbs redir, str parent, dhash_stat status, ptr<insert_info> i);
  void after_new_dir_block(cbs redir, str parent, 
			   dhash_stat status, ptr<insert_info> i);
  void after_appended_new_dirhash(cbs redir, str parent, 
				  dhash_stat status, ptr<insert_info> i);

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
  bool errors() { return error; }
  void root_test();
};

#endif
