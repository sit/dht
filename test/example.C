/*
 * example.{C,h} --- example DHash test that uses testmaster to store some
 *                   data through one node, and retrieves it through another.
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

#include "async.h"
#include "test.h"
#include "testmaster.h"

// all testslaves
testslave slaves[] = {
  { "localhost", DEFAULT_PORT },   // id: 0
  { "localhost", DEFAULT_PORT+1 }, // id: 1
  { "", 0 }
};

char buf[10] = "xxxxxxxxx";
void starttest();
void insert_cb(bool, chordID);
void retrieve_cb(ptr<dhash_block>);

testmaster tm;

// sets up testmaster
int
main(int argc, char *argv[])
{
  setprogname(argv[0]);

  tm.setup(slaves, wrap(starttest));
  amain();
}

// store block in DHash
void
starttest()
{
  tm[0]->insert(buf, 10, wrap(insert_cb));
}


// callback immediately retrieves stored block through other node
void
insert_cb(bool failed, chordID id)
{
  if(failed)
    fatal << "test failed\n";
  tm[1]->retrieve(id, DHASH_CONTENTHASH, wrap(retrieve_cb));
}


// compares fetched data with stored data
void
retrieve_cb(ptr<dhash_block> b)
{
  warn << "retrieved : " << b->data << "\n";
  if(str(b->data) == str(buf)) {
    warn << "test OK\n";
    exit(-1);
  } else {
    warn << "test failed\n";
    exit(0);
  }
}
