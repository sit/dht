/*
 *
 * Copyright (C) 2001 Frank Dabek (fdabek@pdos.lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff

searchcb_entry *
vnode::registerSearchCallback(cbsearch_t cb) 
{
  warn << "registered a search callback\n";
  searchcb_entry *scb = New searchcb_entry (cb);
  scb->cb = cb;
  searchCallbacks.insert_head(scb);
  return scb;
}

void
vnode::testSearchCallbacks(chordID id, chordID target, cbtest_t cb) 
{
  tscb(id, target, searchCallbacks.first, cb);
}

void
vnode::tscb (chordID id, chordID x, searchcb_entry *scb, cbtest_t cb) {
  if (scb == NULL) 
    cb (0);
  else
    (scb->cb (id, x, wrap(this, &vnode::tscb_cb, id, x, scb, cb)));
}

void
vnode::tscb_cb (chordID id, chordID x, 
	      searchcb_entry *scb, cbtest_t cb, int result) {

  searchcb_entry *next = searchCallbacks.next (scb);
  if (result) 
    cb (1);
  else 
    tscb(id, x, next, cb);
}

void
vnode::registerActionCallback(cbaction_t cb) 
{
  actionCallbacks.push_back(cb);
}  

void
vnode::doActionCallbacks(chordID id, char action) 
{
  warnt("CHORD: doActionCallbacks");
  for (unsigned int i=0; i < actionCallbacks.size (); i++)
    actionCallbacks[i] (id, action);
}

void
vnode::removeSearchCallback(searchcb_entry *scb) {
  searchCallbacks.remove (scb);
}
