#include <ihash.h>
#include <skiplist.h>

struct hashID;
struct location;
class block_status_manager;

class block_status {
  friend block_status_manager;
  ihash_entry<block_status> hlink;
  sklist_entry<block_status> sklink;
  chordID id;
  vec<ptr<location> > missing; 

public:
  block_status (chordID b) : id (b) {};
  void missing_on (ptr<location> l);
  void found_on (ptr<location> l);
};

class block_status_manager {
  ihash<chordID, block_status, &block_status::id, &block_status::hlink, hashID> blocks;
  skiplist<block_status, chordID, &block_status::id, &block_status::sklink> sblocks;
  
public:
  block_status_manager ();
  void add_block (const chordID &b);
  void del_block (const chordID &b);
  void missing (ptr<location> remote, const chordID &b);
  void unmissing (ptr<location> remote, const chordID &b);
  chordID next_block (const chordID &b);
  const vec<ptr<location> > where_missing (const chordID &b);
  int mcount (const chordID &b);
};
