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

//#define DEBUG
#include "file.h"

void
melody_file::write_cb (dhash_stat status, ptr<insert_info> i)
{
#ifdef DEBUG
  warn << (int)this << " write_cb " << i->key << "\n";
#endif
  if (status != DHASH_OK) {
    warn << "melody_file store error\n";
    (*error_cb)(); //FIXME more/better error reporting?
  }
  blocks++;
  outstanding--;
  assert((outstanding<1000)&&(outstanding>=0));

  if((outstanding < 20) && sleeping.first) { // FIXME tune 20?
    sleeping.first->readcb_wakeup();
    sleeping.remove(sleeping.first);
  }
}

void
melody_file::close()
{
  warn << "melody_file::close()\n";
  flush();
  vstack->close(wsize);
}

void
melody_file::flush()
{
  int len = wbuf.resid();
  if(wbuf.resid() > 0) {
  assert((outstanding<1000)&&(outstanding>=0));
  outstanding++;
    wbuf.copyout(cbuf.data, wbuf.resid());
    cbuf.type = 1;
    cbuf.offset = wsize - len;
    cbuf.size = len;
    wbuf.rembytes(wbuf.resid());
    
    bigint hash = compute_hash (&cbuf, len + 12);
    dhash->insert((char *)&cbuf, len + 12, wrap(mkref(this), &melody_file::write_cb));

    vstack->add_hash_s(&hash);
#ifdef DEBUG
    warn << (int)this << " flush" << hash << "\n";
#endif
  }
}
void 
melody_file::write(const char *buf, int len)
{
  wsize += len;
  wbuf.copy(buf, len);

  if(wbuf.resid() >= BLOCKPAYLOAD) {
  assert((outstanding<1000)&&(outstanding>=0));
  outstanding++;
    wbuf.copyout(cbuf.data, BLOCKPAYLOAD);
    wbuf.rembytes(BLOCKPAYLOAD);
    cbuf.type = 1;
    cbuf.offset = wsize - len;
    cbuf.size = len;
    
    bigint hash = compute_hash (&cbuf, len + 12);
    dhash->insert((char *)&cbuf, len + 12, wrap(mkref(this), &melody_file::write_cb));

    vstack->add_hash_s(&hash);
#ifdef DEBUG
    warn << (int)this << " write " << hash << " " << len << "\n";
#endif
  }
}

void
melody_file::openw(callback<void, int, bigint>::ptr done_cb, callback<void>::ptr ecb)
{
  blocks = 0;
  wsize = 0;
  error_cb = ecb;
  vstack = New venti_block(dhash, done_cb);
}

void
melody_file::find_venti_depth(int asize) {
#ifdef DEBUG
  warn << "fvd\n";
#endif
  size = asize;
  venti_depth = 1;
  int file_blocks = size / BLOCKPAYLOAD;
  int file_blocks_hashsize = file_blocks * sha1::hashsize;

  unsigned int cursize = file_blocks_hashsize, curblocks;

  while(cursize > BLOCKPAYLOAD) {
    venti_depth++;
    curblocks = cursize / BLOCKPAYLOAD;
    cursize = curblocks * sha1::hashsize;
  }

  warn << "venti_depth " << venti_depth << " for size " << size << "\n";
}

void
melody_file::next_venti_cb(int index, callback<void, int, str>::ref ready_cb, str filename)
{
#ifdef DEBUG
  warn << "next_venti_cb\n";
#endif
  index++;
  if(index < venti_depth)
    vstack = New venti_block(dhash, vstack, wrap(mkref(this), &melody_file::next_venti_cb, index, ready_cb, filename));
  else if(index == venti_depth)
    ready_cb(size, filename);
  else
    warn << "venti_depth and index error\n";
}

void
melody_file::venti_cb(callback<void, int, str>::ref ready_cb, str filename, ptr<dhash_block> blk)
{
#ifdef DEBUG
  warn << "venti_cb\n";
#endif
  blocks++;
  find_venti_depth(((struct melody_block *)blk->data)->offset);

  vstack = New venti_block(dhash, ((struct melody_block *)blk->data), NULL);
  next_venti_cb(0, ready_cb, filename);
}

void
melody_file::openr(bigint filehash, callback<void, const char *, int, int>::ptr rcb, callback<void, int, str>::ref ready_cb, str filename)
{
  vstack = NULL;
  blocks = 0;
  read_cb = rcb;
#ifdef DEBUG
  warn << "trying to retrieve " << filehash << "\n";
#endif
  dhash->retrieve (filehash, wrap(mkref(this), &melody_file::venti_cb, ready_cb, filename));
}

void
melody_file::skip(int blocks)
{
  vstack->skip(blocks, 0);
}

void
melody_file::next()
{
  vstack->get_block(&cbuf, wrap(mkref(this), &melody_file::next_cb));
}

void
melody_file::next_cb(int offset)
{
  if(readgo)
    (*read_cb)(cbuf.data, cbuf.size, offset);
}

void
melody_file::readstop()
{
  readgo = false;
}

melody_file::melody_file(str csock, callback<void, str>::ptr scb)
{
#ifdef DEBUG
#endif
  warn << "cf1\n";
  dhash = New dhashclient(csock);
  statuscb = scb;
  vstack = NULL;
  outstanding = 0;
  readgo = true;
}

melody_file::~melody_file()
{
  warn << (int)this << " mf gone\n";
  delete(dhash);
  delete(vstack);
}

bool
melody_file::sleeptest(cs_client *c) {
  assert((outstanding<1000)&&(outstanding>=0));
  if(outstanding > 20) {
    sleeping.insert_tail(c);
    return true;
  } else
    return false;
}

void
melody_file::sleepdied(cs_client *c) {
  if(sleeping.first)
    sleeping.remove(c);
}
