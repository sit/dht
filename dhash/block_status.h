#ifndef __BLOCK_STATUS_H__
#define __BLOCK_STATUS_H__

#include <ihash.h>
#include <skiplist.h>

struct hashID;
struct location;
class block_status_manager;

class block_status {
  friend class block_status_manager;
  ihash_entry<block_status> hlink;
  sklist_entry<block_status> sklink;
  chordID id;
  vec<ptr<location> > missing; 

public:
  block_status (chordID b) : id (b) {};
  bool is_missing_on (chordID n);
  void missing_on (ptr<location> l);
  void found_on (ptr<location> l);
  void print ();
};

class block_status_manager {
  ihash<chordID, block_status, &block_status::id, &block_status::hlink, hashID> blocks;
  skiplist<block_status, chordID, &block_status::id, &block_status::sklink> sblocks;
  
private:
  chordID my_id;

public:
  block_status_manager (chordID me);
  void add_block (const chordID &b);
  void del_block (const chordID &b);
  void missing (ptr<location> remote, const chordID &b);
  const vec<chordID> missing_what (const chordID &n);
  bool  missing_on (const chordID &b, chordID &n);

  const ptr<location> best_missing (const chordID &b, vec<ptr<location> > succs);
  void unmissing (ptr<location> remote, const chordID &b);
  chordID first_block ();
  chordID next_block (const chordID &b);
  const vec<ptr<location> > where_missing (const chordID &b);
  u_int mcount (const chordID &b);
  u_int pcount (const chordID &b, vec<ptr<location> > succs);
  void status ();
  void print ();
};

#endif
