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

#include "dir.h"
#include "dhash.h"
#include <string.h>
#include "rxx.h"
#include "vec.h"
#include "crypt.h"

dir::dir(melody_file *acc, callback<void, const char *, int, int>::ptr afileout, callback<void, int>::ptr afilehead, cs_client *acs)
{
  cc = acc;
  fileout = afileout;
  filehead = afilehead;
  cs = acs;
  char tmp[sha1::hashsize] = DIRROOT;
  mpz_set_rawmag_be(&dirhash, tmp, sha1::hashsize);
}

void
dir::root_test()
{
  cc->dhash->retrieve (dirhash, DHASH_APPEND, wrap (this, &dir::root_test_cb));
}

void
dir::root_test_cb(ptr<dhash_block> blk)
{
  if(!blk) {
    struct dir_record dr;
    dr.type = 0;
    mpz_get_raw (dr.key, sha1::hashsize, &dirhash);
    strlcpy(dr.entry, "..", 256);
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, true, wrap(this, &dir::null)));
  }
}

dir::~dir()
{
  warn << (int)cs << " delete dir " << "\n";
  delete cc;
}

static rxx pathrx ("[^/]+", "i");
void
dir::opendir(str path, callback<void>::ptr agotdir)
{
  gotdir = agotdir;
  char tmp[sha1::hashsize] = DIRROOT;
  bigint root;
  mpz_set_rawmag_be(&root, tmp, sha1::hashsize);

  vec<str> pathelm;
  strbuf tmpath = path;
  while(pathrx.search(tmpath)) {
    pathelm.push_back(pathrx[0]);
    tmpath.tosuio()->rembytes(pathrx.end(0));
  }

  opendir(pathelm, root);
}

void
dir::opendir(vec<str> pathelm, bigint dir)
{
  if(pathelm.size() > 0)
    cc->dhash->retrieve (dir, DHASH_APPEND, wrap (this, &dir::find_entry, pathelm));
  else
    cc->dhash->retrieve (dir, DHASH_APPEND, wrap (this, &dir::found_entry));
}

void
dir::find_entry(vec<str> pathelm, ptr<dhash_block> blk)
{
  if(!blk) {
    warn << (int)cs << " no such path found: " << pathelm.front() << "\n";
    // FIXME error reporting
    return;
  }
  buf.copy(blk->data, blk->len);
  unsigned int name_index = 0;

  while(name_index < blk->len) {
    struct dir_record *di = (struct dir_record *) (((char *)blk->data) + name_index);

    if(pathelm.front() == di->entry) {
      pathelm.pop_front();
      if(pathelm.size() > 0) {
	bigint tmp;
	mpz_set_rawmag_be(&tmp, di->key, sha1::hashsize);
	opendir(pathelm, tmp);
      } else {
	mpz_set_rawmag_be(&dirhash, di->key, sha1::hashsize);
	warn << (int)cs << " found " << dirhash << "\n";
	if(di->type == 0)
	  cc->dhash->retrieve (dirhash, DHASH_APPEND, wrap (this, &dir::found_entry));
	else {
	  cc->openr(dirhash, fileout, filehead);
	}
      }
      buf.clear();
      return;
    }
    name_index += sizeof(struct dir_record);
  }
  buf.clear();

  // FIXME failure
  exist = false;
  gotdir();
}

void
dir::found_entry(ptr<dhash_block> blk)
{
  if(blk) {
    buf.copy(blk->data, blk->len);
    entry_index = 0;
    exist = true;
    gotdir();
  } else
    warn << (int)cs << " block gone\n"; // FIXME add mroe error recovery
}

void
dir::add_dir(str dir, str parent, cbv redir)
{
  bigint tmphash = random_bigint(sha1::hashsize*8);
  warn << (int)cs << " trying to add " << dir << ": " << tmphash << " to " << parent << ":" << dirhash << "\n";

  strbuf tmp = parent;
  tmp << dir;
  warn << (int)cs << " looking to see if " << tmp << " exists\n";
  opendir(tmp, wrap(this, &dir::add, dir, 0, tmphash, 0, redir));
}

void
dir::add_file(str file, str parent, bigint filehash, int size, cbv redir)
{
  warn << (int)cs << " trying to add " << file << " to " << dirhash << "\n";
  strbuf tmp = parent;
  tmp << file;
  warn << (int)cs << " looking to see if " << tmp << " exists\n";
  opendir(tmp, wrap(this, &dir::add, file, 1, filehash, size, redir));
}

void
dir::null() {}

void
dir::add(str name, int type, bigint tmphash, int size, cbv redir)
{
  if(exist) {
    exist = false;
    return; // FIXME change file name to new "version"
  }
  struct dir_record dr;
  dr.type = type;
  dr.size = size;
  strlcpy(dr.entry, name.cstr(), 256);
  mpz_get_raw (dr.key, sha1::hashsize, &tmphash);
  if(type == 0)
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, false, wrap(this, &dir::add2, type, tmphash, size, redir)));
  else
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, true, redir));
}

void
dir::add2(int type, bigint tmphash, int size, cbv redir) {
  struct dir_record dr;
  dr.type = type;
  dr.size = size;
  strlcpy(dr.entry, "..", 256);
  mpz_get_raw (dr.key, sha1::hashsize, &dirhash);
  cc->dhash->append (tmphash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, true, redir));
}

void
dir::flush_cb(bool done, cbv redir, bool error, chordID key)
{
  warn << (int)cs << " flush_cb " << (int)this << "\n";
  if (error)
    warn << (int)cs << " dir store error\n";
  buf.clear();
  redir();
  if(done)
    delete this;
}

bool
dir::more(void)
{
  return sizeof(struct dir_record) <= buf.resid();
}

void
dir::readdir(struct dir_record *dr)
{
  buf.copyout(dr, sizeof(struct dir_record));
  buf.rembytes(sizeof(struct dir_record));
  warn << (int)cs << " ei " << entry_index << " diel " << dr->entry << " sdr " << sizeof(struct dir_record) << " bl " << buf.resid() << "\n";
  entry_index += sizeof(struct dir_record);
}
