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

venti_block::venti_block(dhashclient *dh, melody_block *bl, venti_block *ap)
{
#ifdef DEBUG
  warn << "venti_block1\n";
#endif
  memcpy(&data, bl, bl->size);
  more_init(ap, 0);
  dhash = dh;
}

venti_block::venti_block(dhashclient *dh, venti_block *ap, cbv acb)
{
#ifdef DEBUG
  warn << "venti_block2c\n";
#endif
  warn << (int)this << " venti_block2c\n";
  ap->get_block(&data, wrap(this, &venti_block::more_init_gb, ap, acb));
  dhash = dh;
}

venti_block::venti_block(dhashclient *dh, callback<void, int, bigint>::ptr dcb)
{
#ifdef DEBUG
  warn << "venti_block3\n";
  warn << "new venti_block for size " << asize << "\n";
#endif
  warn << (int)this << " venti_block3\n";
  data.type = 2;
  data.size = 12;
  more_init(NULL, 0);
  dhash = dh;
  done_cb = dcb;
}

void
venti_block::more_init(venti_block *ap, int dummy)
{
#ifdef DEBUG
  warn << "more_init\n";
#endif
  if(data.type != 2)
    warn << "block not venti type\n";
  hashindex = data.data;
  parent = ap;
  offset = 0;
  done = false;
}

void
venti_block::more_init_gb(venti_block *ap, callback<void>::ref acb, int dummy)
{
#ifdef DEBUG
  warn << "more_init_gb_nc\n";
#endif
  more_init(ap, dummy);
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
      //      strbuf foo;
      //      foo << "retrieved";
      //      (*conn->statuscb) (foo);
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
  dhash->retrieve (blockhash, wrap (this, &venti_block::get_block_cb, bl, cb, of));
}

// final function in get_block chain.
void
venti_block::get_block_cb(melody_block *bl, cbi cb, int of, ptr<dhash_block> blk)
{
#ifdef DEBUG
  warn << "gb_cb\n";
#endif
  if(blk) {
    if(blk->len > BLOCKSIZE)
      warn << "gb_cb block too big\n";
    else
      memcpy(bl, blk->data, blk->len);
    cb(of);
  } else
    // FIXME error?
    ;
}

void
venti_block::reset_cb (cbv after, dhash_stat status, ptr<insert_info> i)
{
#ifdef DEBUG
  warn << "reset_cb\n";
#endif
  warn << (int)this << " reset_cb\n";

  if (status != DHASH_OK)
    warn << "venti_block store error\n";
  if(done) {
    //    strbuf foo;
    //    foo << "stored " << conn->blocks << " blocks";
    //    warn << "reset_cb\n";// << (int)conn << "\n";
    //    (*conn->statuscb) (foo);
  }
  after();
}

void
venti_block::reset(cbv after)
{
  if(parent == NULL)
    parent = New venti_block(dhash, done_cb);
  warn << (int)this << " reset to " << (int)parent << "\n";

  bigint mehash = compute_hash (&data, data.size);
#ifdef DEBUG
  warn << "reset " << mehash << "\n";
#endif
  parent->add_hash(&mehash, wrap(&null));
  dhash->insert ((char *)&data, data.size, wrap (this, &venti_block::reset_cb, after));

  hashindex = data.data;
  data.size = 12;
}

void
venti_block::reset_cb_s (dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "venti_block reset_cb_s store error\n";
}

void
venti_block::reset_s()
{
  if(parent == NULL)
    parent = New venti_block(dhash, done_cb);
  warn << (int)this << " reset to " << (int)parent << "\n";

  bigint mehash = compute_hash (&data, data.size);
#ifdef DEBUG
  warn << "reset " << mehash << "\n";
#endif
  parent->add_hash(&mehash, wrap(&null));
  dhash->insert ((char *)&data, data.size, wrap (this, &venti_block::reset_cb_s));

  hashindex = data.data;
  data.size = 12;
}

//#define DEBUG

void
venti_block::close_cb (dhash_stat status, ptr<insert_info> i)
{
#ifdef DEBUG
  warn << "close_cb\n";
#endif
  warn << (int)this << " close_cb\n";

  if (status != DHASH_OK)
    warn << "venti_block store error\n";
  if(done) {
    //    strbuf foo;
    //    foo << "stored " << conn->blocks << " blocks";
    //    warn << "close_cb\n";// << (int)conn << "\n";
    //    (*conn->statuscb) (foo);
  }
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
  warn << (int)this << " close vb " << mehash << "\n";
  dhash->insert ((char *)&data, data.size, wrap (this, &venti_block::close_cb));

  if(parent) {
    parent->add_hash(&mehash, wrap(parent, &venti_block::close, size));
    //    parent->close(size); // FIXME, make async after the add_hash/reset_cb
  } else {
    warn << "hash: " << mehash << "\n";
    done = true;
    (*done_cb)(size, mehash);
    warn << "NO MORE MEHASH\n";
  }
}

void
venti_block::add_hash(bigint *hash, cbv after)
{
  assert(hashindex);

#ifdef DEBUG
  warn << "add_hash " << *hash << "\n";
#endif
  mpz_get_raw (hashindex, sha1::hashsize, hash);
  hashindex += sha1::hashsize;
  data.size += sha1::hashsize;

  if(full())
    reset(after);
  else
    after();
}
void
venti_block::add_hash_s(bigint *hash)
{
  assert(hashindex);

#ifdef DEBUG
  warn << "add_hash " << *hash << "\n";
#endif
  mpz_get_raw (hashindex, sha1::hashsize, hash);
  hashindex += sha1::hashsize;
  data.size += sha1::hashsize;

  if(full())
    reset_s();
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

venti_block::~venti_block()
{
  warn << (int)this << " ~venti_block\n";
  if(parent)
    delete(parent);
}

void null() {}
