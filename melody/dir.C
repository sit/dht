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

dir::dir(ptr<melody_file>acc, callback<void, const char *, int, int>::ptr afileout, callback<void, int, str>::ptr afilehead, cs_client *acs)
{
  cc = acc;
  fileout = afileout;
  filehead = afilehead;
  cs = acs;
  char tmp[sha1::hashsize] = DIRROOT;
  mpz_set_rawmag_be(&vhash, tmp, sha1::hashsize);
  noread = false;
}

void
dir::root_test()
{
  cc->dhash->retrieve (vhash, wrap (this, &dir::root_test_got_rb));
}

void
dir::root_test_got_rb(ptr<dhash_block> blk)
{
  if(!blk) {
    warn << "couldn't find root block\n";
    struct dir_record dr;
    dr.type = 0;
    dr.size = 0;
    gettimeofday(&dr.ctime, NULL);
    mpz_get_raw (dr.key, sha1::hashsize, &vhash);
    strncpy(dr.entry, "..", 256);
    dirhash = random_bigint(sha1::hashsize*8);
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::create_venti, wrap(this, &dir::root_done), str("")));
  } else
    root_done("");
}

void
dir::root_done(str foo)
{
  delete(cc);
  delete(this);
};

dir::~dir()
{
  warn << (int)cs << " delete dir\n";
}

static rxx pathrx ("^/+([^/;]+;[\\dabcdef]+)", "i");
//static rxx pathrx ("[^/]+", "i");
void
dir::opendir(str path, callback<void>::ptr agotdir, bool anoread)
{
  gotdir = agotdir;
  noread = anoread;
  error = false;
  char tmp[sha1::hashsize] = DIRROOT;
  bigint root;
  mpz_set_rawmag_be(&root, tmp, sha1::hashsize);
  warn << root << "\n";

  strbuf tmpath = path;
  while(pathrx.search(tmpath)) {
    warn << " push " << pathrx[1] << "\n";
    pathelm.push_back(pathrx[1]);
    tmpath.tosuio()->rembytes(pathrx.end(0));
  }

  if((tmpath.tosuio()->resid() > 0) &&
     !((tmpath.tosuio()->resid() == 1) &&
       (((str)tmpath)[0] == '/'))) {
    error = true;
    warn << "extra " << tmpath << "\n";
  }

  opendir(root);
}

//loops:
// open elements of path, for each dir,
//  read hash from venti block, for each hash,
//   read dir block... do find_entry

void
dir::opendir(bigint dir)
{
  vhash = dir;
  if(pathelm.size() > 0)
    cc->dhash->retrieve (dir, wrap (this, &dir::opendir_got_venti, wrap(this, &dir::find_entry)));
  else if(noread)
    cc->dhash->retrieve (dir, wrap (this, &dir::opendir_got_venti_noread));
  else
    cc->dhash->retrieve (dir, wrap (this, &dir::opendir_got_venti, wrap(this, &dir::found_entry)));
}

void
dir::opendir_got_venti(cbretrieve_t cbr, ptr<dhash_block> blk)
{
  if(!blk) {
    warn << (int)cs << " no such path found:";
    if(pathelm.size() > 0)
      warnx << pathelm.front();
    warnx << "\n";
    // FIXME error reporting
      error = true;
      gotdir();
    return;
  }

  struct melody_block *mb = (struct melody_block *)blk->data;

  if(mb->type != 2) {
    warn << (int)cs << " not venti type dir\n";
      error = true;
      gotdir();
    return;
  }

  cbuf.copy(blk->data, blk->len);
  cbuf.rembytes(sizeof(int)*3);
  next_dirblk(cbr);
}

void
dir::opendir_got_venti_noread(ptr<dhash_block> blk)
{
  if(!blk) {
    warn << (int)cs << " no such path found\n";
    // FIXME error reporting
      error = true;
      gotdir();
    return;
  }

  struct melody_block *mb = (struct melody_block *)blk->data;

  if(mb->type != 2) {
    warn << (int)cs << " not venti type dir\n";
      error = true;
      gotdir();
    return;
  }

  cbuf.copy(blk->data, blk->len);
  cbuf.rembytes(sizeof(int)*3);

  while(cbuf.resid() >= 2*sha1::hashsize)
    cbuf.rembytes(sha1::hashsize);
  char tmp[sha1::hashsize];
  cbuf.copyout(tmp, sha1::hashsize);
  mpz_set_rawmag_be(&dirhash, tmp, sha1::hashsize);
  warn << "opened " << dirhash << ", " << vhash << "\n";
  exist = true;
  gotdir();
}

void
dir::next_dirblk(cbretrieve_t cbr) {
  char tmp[sha1::hashsize];
  cbuf.copyout(tmp, sha1::hashsize);
  cbuf.rembytes(sha1::hashsize);
  mpz_set_rawmag_be(&dirhash, tmp, sha1::hashsize);
  warn << "opened " << dirhash << ", " << vhash << "\n";
  cc->dhash->retrieve (dirhash, cbr);
}

static rxx namechashrx ("(.+);([\\dabcdef]+)", "i");
void
dir::find_entry(ptr<dhash_block> blk)
{
  unsigned int name_index = 0;
  warn << "find_entry\n";

  if(!blk) { warn << "D:find_entry no blk\n";       error = true;
      gotdir();
return; }

  while(name_index < blk->len) {
    struct dir_record *di = (struct dir_record *) (((char *)blk->data) + name_index);
    if(!namechashrx.search(pathelm.front())) {
      // FIXME error reporting
      warn << (int)cs << " bad path: " << pathelm.front() << "\n";
      error = true;
      gotdir();
      return;
    }

    bigint pathhash(namechashrx[2],16), dehash; // ??
    mpz_set_rawmag_be(&dehash, di->key, sha1::hashsize);
    //warn << (int)cs << " de " << di->entry << ":" << dehash << "\n";
    //warn << (int)cs << " d2 " << namechashrx[1] << ":" << pathhash << "\n";

    if((namechashrx[1] == di->entry) &&
       (pathhash == dehash)) {
      pathelm.pop_front();
      if(pathelm.size() > 0) {
	if(di->type == 0)
	  opendir(dehash);
	else {
	  warn << (int)cs << " whaaa? trying to read non-dir type block\n";
	  error = true;
	  gotdir();
	}
      } else {
	mpz_set_rawmag_be(&dirhash, di->key, sha1::hashsize);
	warn << (int)cs << " found " << dirhash << "\n";
	if(di->type == 0)
	  opendir(dirhash);
	else if(di->type == 1) {
	  cc->openr(dirhash, fileout, filehead, di->entry);
	}
      }
      return;
    }
    name_index += sizeof(struct dir_record);
  }

  if(cbuf.resid() < sha1::hashsize) {
  // FIXME failure
    exist = false;
    error = true;
    gotdir();
  } else
    next_dirblk(wrap(this, &dir::find_entry));
}

void
dir::found_entry(ptr<dhash_block> blk)
{
  if(!blk) {
    warn << (int)cs << " block gone\n"; // FIXME add mroe error recovery
      error = true;
      gotdir();
    return;
  }
  buf.copy(blk->data, blk->len);

  if(cbuf.resid() < sha1::hashsize) {
    entry_index = 0;
    exist = true;
    gotdir();
  } else
    next_dirblk(wrap(this, &dir::found_entry));
}

void
dir::add_dir(str dir, str parent, cbs redir)
{
  bigint tmphash = random_bigint(sha1::hashsize*8);
  combdir << parent << dir << ";" << tmphash;
  warn << (int)cs << " trying to add " << dir << ": " << tmphash << " to " << parent << ":" << dirhash << "\n";
  add(dir, 0, tmphash, 0, redir, combdir);
}

void
dir::add_file(str file, str parent, bigint filehash, int size, cbs redir)
{
  warn << (int)cs << " trying to add " << file << " to " << dirhash << "\n";
  add(file, 1, filehash, size, redir, parent);
}

void
dir::add(str name, int type, bigint tmphash, int size, cbs redir, str parent)
{
  struct dir_record dr;
  dr.type = type;
  dr.size = size;
  gettimeofday(&dr.ctime, NULL);
  strncpy(dr.entry, name.cstr(), 256);
  mpz_get_raw (dr.key, sha1::hashsize, &tmphash);
  buf.clear();
  buf.copy(&dr, sizeof(dr));

  if(type == 0)
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, wrap(this, &dir::add2, tmphash, size, redir), parent));
  else
    cc->dhash->append (dirhash, (char *)&dr, sizeof(dr), wrap (this, &dir::flush_cb, redir, parent));
}

void
dir::add2(bigint tmphash, int size, cbs redir, str parent) {
  struct dir_record dr;
  dr.type = 0;
  dr.size = size;
  gettimeofday(&dr.ctime, NULL);
  strncpy(dr.entry, "..", 256);
  mpz_get_raw (dr.key, sha1::hashsize, &vhash);
  vhash = tmphash;
  cc->dhash->append (random_bigint(sha1::hashsize*8), (char *)&dr, sizeof(dr), wrap (this, &dir::create_venti, redir, parent));
}

// only creating one layer venti, so dirs are limited to ~700000 entries
void
dir::create_venti(cbs redir, str parent, dhash_stat status, ptr<insert_info> i)
{
  if(status != DHASH_OK) {
    warn << "can't add dir block\n";
    exit(1);
  }

  struct {
    int type, offset, size;
    char tmp[sha1::hashsize];
  } mb;
  mb.type = 2;
  mb.offset = 0;
  mb.size = 0;
  mpz_get_raw (mb.tmp, sha1::hashsize, &i->key);
  cc->dhash->append(vhash, (char *)&mb, sizeof(mb), wrap(this, &dir::create_venti_done, redir, parent));
}

void
dir::create_venti_done(cbs redir, str parent, dhash_stat status, ptr<insert_info> i) {
  if(status != DHASH_OK) {
    warn << "can't add venti block\n";
    exit(1);
  }
  redir(parent);
}

void
dir::flush_cb(cbs redir, str parent, dhash_stat status, ptr<insert_info> i)
{
  warn << (int)cs << " flush_cb " << (int)this << "\n";
  if (status != DHASH_OK) {
    //    warn << (int)cs << " dir store error\n";
    // existing dir block is probably full
    // Ok, let's add another dir block
    warn << (int)cs << " but it's ok, we need to add new dir block\n";
    struct dir_record dr;
    buf.copyout(&dr, sizeof(struct dir_record));
    bigint tmphash = random_bigint(sha1::hashsize*8);
    cc->dhash->append (tmphash, (char *)&dr, sizeof(dr), wrap (this, &dir::after_new_dir_block, redir, parent));
  } else
    redir(parent);
}

void
dir::after_new_dir_block(cbs redir, str parent, 
			 dhash_stat status, ptr<insert_info> i) {
  if(status != DHASH_OK) {
    warn << (int)cs << " can't add new dir block.\n";
    exit(1);
  }
  char tmp[sha1::hashsize];
  mpz_get_raw (tmp, sha1::hashsize, &i->key);
  cc->dhash->append(vhash, tmp, sha1::hashsize, wrap(this, &dir::after_appended_new_dirhash, redir, parent));
}

void
dir::after_appended_new_dirhash(cbs redir, str parent, 
				dhash_stat status, ptr<insert_info> i) {
  if(status != DHASH_OK) {
    warn << (int)cs << " can't append new dirhash.\n";
    exit(1);
  }
  redir(parent);
}

bool
dir::more(void)
{
  return(sizeof(struct dir_record) <= buf.resid());
}

void
dir::readdir(struct dir_record *dr)
{
  buf.copyout(dr, sizeof(struct dir_record));
  buf.rembytes(sizeof(struct dir_record));
  //  warn << (int)cs << " ei " << entry_index << " diel " << dr->entry << " sdr " << sizeof(struct dir_record) << " bl " << buf.resid() << "\n";
  entry_index += sizeof(struct dir_record);
}
