/*
 * example2.C --- insert, isolate, failed retrieve, un-isolate, succesful
 * retrieve.
 *
 * Copyright (C) 2002  Thomer M. Gil (thomer@lcs.mit.edu)
 *   		       Massachusetts Institute of Technology
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
 */

#include "testmaster.h"

char buf[10] = "xxxxxxxxx";

void 
done(int *n, int ok)
{
  if(!ok)
    fatal << "error\n";

  if(!--*n)
    exit(0);
}

// compares fetched data with stored data
void
retrieve_cb(ptr<insert_info> i, ptr<dhash_block> b)
{
  warn << "this should fail\n";
  warn << "retrieved : " << b->data << "\n";

  if(str(b->data) == str(buf)) {
    warn << "test OK\n";
    exit(-1);
  } else {
    warn << "test failed\n";
    exit(0);
  }
}

void
isolate_cb(testmaster *tm, ptr<insert_info> i, int ok)
{
  warn << "ok = " << ok << "\n";
  (*tm)[1]->retrieve(i->key, wrap(retrieve_cb, i));
}


// callback immediately retrieves stored block through other node
void
insert_cb(testmaster *tm, dhash_stat status, ptr<insert_info> i)
{
  if(status != DHASH_OK || !i)
    fatal << "test failed\n";

  tm->instruct(1, ISOLATE, wrap(isolate_cb, tm, i));
}


void
start_test(testmaster *tm)
{
  // tm->unblock(0, 1, wrap(&done, &n));
  (*tm)[0]->insert(buf, 10, wrap(insert_cb, tm));
}

int
main(int argc, char *argv[])
{
  if(argc != 2)
    fatal << "usage:\n\texample2 [config file]\n";

  testmaster *tm = New testmaster(argv[1]);
  tm->setup(wrap(start_test, tm));
  amain();
}
