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
#include "block.h"

venti_block::venti_block(melody_file *ac, melody_block *bl)
{
#ifdef DEBUG
  warn << "venti_block1\n";
#endif
  memcpy(&data, bl, bl->size);
  more_init(ac, NULL, 0);
}

venti_block::venti_block(melody_file *ac, venti_block *ap)
{
#ifdef DEBUG
  warn << "venti_block2\n";
#endif
  ap->get_block(&data, wrap(this, &venti_block::more_init, ac, ap));
}

venti_block::venti_block(melody_file *ac, venti_block *ap, callback<void>::ref acb)
{
#ifdef DEBUG
  warn << "venti_block2c\n";
#endif
  ap->get_block(&data, wrap(this, &venti_block::more_init_gb, ac, ap, acb));
}

venti_block::venti_block(melody_file *ac, callback<void, int, bigint>::ptr dcb)
{
#ifdef DEBUG
  warn << "venti_block3\n";
  warn << "new venti_block for size " << asize << "\n";
#endif
  data.type = 2;
  data.size = 12;
  more_init(ac, NULL, 0);
  done_cb = dcb;
}

void
venti_block::more_init(melody_file *ac, venti_block *ap, int dummy)
{
#ifdef DEBUG
  warn << "more_init\n";
#endif
  if(data.type != 2)
    warn << "block not venti type\n";
  hashindex = data.data;
  conn = ac;
  parent = ap;
  offset = 0;
  done = false;
}

void
venti_block::more_init_gb(melody_file *ac, venti_block *ap, callback<void>::ref acb, int dummy)
{
#ifdef DEBUG
  warn << "more_init_gb_nc\n";
#endif
  more_init(ac, ap, dummy);
  acb();
}

void
venti_block::skip(int blocks, int dummy)
{
  hashindex += sha1::hashsize*blocks;
  offset += blocks*BLOCKPAYLOAD;
  if(empty()) {
    int overflow = hashindex - (data.data + data.size - 12);
    warn << "overflow of " << overflow << "\n";
    assert(!(overflow&0xf));
    overflow /= sha1::hashsize;
    offset -= overflow*BLOCKPAYLOAD;
    parent->get_block(&data, wrap(this, &venti_block::skip, overflow));
  }
}

// refills venti_block if necessary, then gets block
void
venti_block::get_block (melody_block *bl, cbi cb)
{
#ifdef DEBUG
  warn << "get_block bl\n";
#endif
  if(empty()) {
    if(parent == NULL) { // no more data
      strbuf foo;
      foo << "retrieved " << conn->blocks << " blocks";
      (*conn->statuscb) (foo);
    } else {
      parent->get_block(&data, wrap(this, &venti_block::get_block_rc, bl, cb, offset)); // need sync... what does this mean? FIXME
      offset += BLOCKPAYLOAD; // FIXME fixed?
    }
  } else {
    get_block2(bl, cb, offset);
    offset += BLOCKPAYLOAD;
  }
}

// recursive case, does some reset once venti_block is loaded, then continues
void
venti_block::get_block_rc(melody_block *bl, cbi cb, int of, int dummy)
{
  if(data.type != 2)
    warn << "block not venti type\n";
  hashindex = data.data;

  get_block2(bl, cb, of);
}

// real get_block code... now we know where the block is
void
venti_block::get_block2 (melody_block *bl, cbi cb, int of)
{
  assert(hashindex);

  bigint blockhash;
  mpz_set_rawmag_be(&blockhash, hashindex, sha1::hashsize);  // For big endian
  hashindex += sha1::hashsize;

#ifdef DEBUG
  warn << "gb trying to retrieve " << blockhash << "\n";
#endif
  conn->dhash->retrieve (blockhash, DHASH_CONTENTHASH, wrap (this, &venti_block::get_block_cb, bl, cb, of));
}

// final function in get_block chain.
void
venti_block::get_block_cb(melody_block *bl, cbi cb, int of, ptr<dhash_block> blk)
{
#ifdef DEBUG
  warn << "gb_cb\n";
#endif
  conn->blocks++;

  if(blk->len > BLOCKSIZE)
    warn << "gb_cb block too big\n";
  else
    memcpy(bl, blk->data, blk->len);
  cb(of);
}

void
venti_block::reset_cb (bool error, chordID key)
{
#ifdef DEBUG
  warn << "reset_cb\n";
#endif
  conn->blocks++;

  if (error)
    warn << "venti_block store error\n";
  if(done) {
    strbuf foo;
    foo << "stored " << conn->blocks << " blocks";
    warn << "reset_cb " << (int)conn << "\n";
    (*conn->statuscb) (foo);
  }
}

void
venti_block::reset()
{
  if(parent == NULL)
    parent = New venti_block(conn, done_cb);

  bigint mehash = compute_hash (&data, data.size);
#ifdef DEBUG
  warn << "reset " << mehash << "\n";
#endif
  parent->add_hash(&mehash);
  conn->dhash->insert ((char *)&data, data.size, wrap (this, &venti_block::reset_cb));

  hashindex = data.data;
  data.size = 12;
}

//#define DEBUG

void
venti_block::close_cb (bool error, chordID key)
{
#ifdef DEBUG
  warn << "close_cb\n";
#endif
  conn->blocks++;

  if (error)
    warn << "venti_block store error\n";
  if(done) {
    strbuf foo;
    foo << "stored " << conn->blocks << " blocks";
    warn << "close_cb " << (int)conn << "\n";
    (*conn->statuscb) (foo);
  }
  delete(this);
}

void
venti_block::close(int size)
{
  if(parent == NULL)
    data.offset = size;

  bigint mehash = compute_hash (&data, data.size);
#ifdef DEBUG
  warn << "close vb " << mehash << "\n";
#endif
  conn->dhash->insert ((char *)&data, data.size, wrap (this, &venti_block::close_cb));

  if(parent) {
    parent->add_hash(&mehash);
    parent->close(size);
  } else {
    warn << "hash: " << mehash << "\n";
    done = true;
    (*done_cb)(size, mehash);
    warn << "NO MORE MEHASH\n";
  }
}

void
venti_block::add_hash(bigint *hash)
{
  assert(hashindex);

#ifdef DEBUG
  warn << "add_hash " << *hash << "\n";
#endif
  mpz_get_raw (hashindex, sha1::hashsize, hash);
  hashindex += sha1::hashsize;
  data.size += sha1::hashsize;

  if(full())
    reset();
}

bool
venti_block::full()
{
  assert(hashindex);

#ifdef DEBUG
  warn << "full\n";
#endif
  if(hashindex >= (data.data + BLOCKPAYLOAD))
    return true;
  else
    return false;
}

bool
venti_block::empty()
{
  assert(hashindex);

#ifdef DEBUG
  warn << "empty\n";
#endif
  if(hashindex >= (data.data + data.size - 12))
    return true;
  else
    return false;
}


void
melody_file::write_cb (bool error, chordID key)
{
#ifdef DEBUG
  warn << "write_cb\n";
#endif
  if (error) {
    warn << "melody_file store error\n";
    (*error_cb)(); //FIXME more/better error reporting?
  }
  blocks++;
  outstanding--;

  if((outstanding < 20) && sleeping.first) { // FIXME tune 20?
    sleeping.first->readcb_wakeup();
    sleeping.remove(sleeping.first);
  }
}

void
melody_file::close()
{
  flush();
  vstack->close(wsize);
}

void
melody_file::flush()
{
  int len = wbuf.resid();
  if(wbuf.resid() > 0) {
    wbuf.copyout(cbuf.data, wbuf.resid());
    cbuf.type = 1;
    cbuf.offset = wsize - len;
    cbuf.size = len;
    wbuf.rembytes(wbuf.resid());
    
    bigint hash = compute_hash (&cbuf, len + 12);
    dhash->insert ((char *)&cbuf, len + 12, wrap (this, &melody_file::write_cb));

    vstack->add_hash(&hash);
  }
}
void 
melody_file::write(const char *buf, int len)
{
#ifdef DEBUG
  warn << "write " << len << "\n";
#endif
  outstanding++;
  wsize += len;
  wbuf.copy(buf, len);

  if(wbuf.resid() >= BLOCKPAYLOAD) {
    wbuf.copyout(cbuf.data, BLOCKPAYLOAD);
    wbuf.rembytes(BLOCKPAYLOAD);
    cbuf.type = 1;
    cbuf.offset = wsize - len;
    cbuf.size = len;
    
    bigint hash = compute_hash (&cbuf, len + 12);
    dhash->insert ((char *)&cbuf, len + 12, wrap (this, &melody_file::write_cb));

    vstack->add_hash(&hash);
  }
}

void
melody_file::openw(callback<void, int, bigint>::ptr done_cb, callback<void>::ptr ecb)
{
  blocks = 0;
  wsize = 0;
  error_cb = ecb;
  vstack = New venti_block(this, done_cb);
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
melody_file::next_venti_cb(int index, cbi ready_cb)
{
#ifdef DEBUG
  warn << "next_venti_cb\n";
#endif
  index++;
  if(index < venti_depth)
    vstack = New venti_block(this, vstack, wrap(this, &melody_file::next_venti_cb, index, ready_cb));
  else if(index == venti_depth)
    ready_cb(size);
  else
    warn << "venti_depth and index error\n";
}

void
melody_file::venti_cb(cbi ready_cb, ptr<dhash_block> blk)
{
#ifdef DEBUG
  warn << "venti_cb\n";
#endif
  blocks++;
  find_venti_depth(((struct melody_block *)blk->data)->offset);

  vstack = New venti_block(this, ((struct melody_block *)blk->data));
  next_venti_cb(0, ready_cb);
}

void
melody_file::openr(bigint filehash, callback<void, const char *, int, int>::ptr rcb, cbi ready_cb)
{
  vstack = NULL;
  blocks = 0;
  read_cb = rcb;
#ifdef DEBUG
  warn << "trying to retrieve " << filehash << "\n";
#endif
  dhash->retrieve (filehash, DHASH_CONTENTHASH, wrap (this, &melody_file::venti_cb, ready_cb));
}

void
melody_file::skip(int blocks)
{
  vstack->skip(blocks, 0);
}

void
melody_file::next()
{
  vstack->get_block(&cbuf, wrap(this, &melody_file::next_cb));
}

void
melody_file::next_cb(int offset)
{
  (*read_cb)(cbuf.data, cbuf.size, offset);
}

melody_file::melody_file(str csock, callback<void, str>::ptr scb)
{
#ifdef DEBUG
  warn << "cf1\n";
#endif
  dhash = New dhashclient(csock);
  statuscb = scb;
  vstack = NULL;
  outstanding = 0;
}

bool
melody_file::sleeptest(cs_client *c) {
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
