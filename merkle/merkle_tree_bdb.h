#include "merkle_tree.h"

class merkle_node_bdb;

class merkle_tree_bdb : public merkle_tree
{
  friend class merkle_node_bdb;
  DB_ENV *dbe;
  DB *nodedb;
  DB *keydb;

  void warner (const char *method, const char *desc, int r) const;

  // Berkeley DB access methods
  merkle_node_bdb *read_node (u_int depth, const merkle_hash &key, DB_TXN *t = NULL, int flags = 0);
  bool write_node (const merkle_node_bdb *node, DB_TXN *t = NULL);
  bool del_node (u_int depth, const merkle_hash &key, DB_TXN *t = NULL);
  bool check_key (const merkle_hash &key, DB_TXN *t = NULL);
  int insert_key (const merkle_hash &key, DB_TXN *t = NULL);
  int remove_key (const merkle_hash &key, DB_TXN *t = NULL);

  void verify_subtree (merkle_node_bdb *n, DB_TXN *t);

  int get_hash_list (vec<merkle_hash> &keys,
      u_int depth, const merkle_hash &prefix, DB_TXN *t = NULL);

  // Not relevant but must be implemented.
  // This suggests a bad abstraction.
  int insert (u_int depth, merkle_hash &key, merkle_node *n) {
    return 0;
  }
  void remove (u_int depth, merkle_hash &key, merkle_node *n) {}

public:
  merkle_tree_bdb (const char *path, bool join, bool ro);
  virtual ~merkle_tree_bdb ();

  // Sub-classes must implement the following methods
  merkle_node *get_root ();
  int insert (merkle_hash &key);
  void remove (merkle_hash &key);
  bool key_exists (chordID key);
  vec<merkle_hash> database_get_keys (u_int depth,
      const merkle_hash &prefix);
  vec<chordID> get_keyrange (chordID min, chordID max, u_int n);

  using merkle_tree::lookup;
  merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int *depth, u_int max_depth,
			       const merkle_hash &key);

  // Sub-classes may override the following methods
  void lookup_release (merkle_node *n);
  void sync (bool reopen = true);
  void check_invariants ();
};

struct merkle_node_bdb : public merkle_node
{
  merkle_hash prefix;
  u_int32_t depth;

  bool leaf;
  merkle_hash _child_hash[64];

  merkle_tree_bdb *tree;

  vec<merkle_node_bdb *> to_delete;

  // Must make sure to update this if parent gets more fields.
  static const size_t marshaled_size =
    2 * sizeof (u_int32_t) + 4 /* bool */ + (2 + 64) * merkle_hash::size;

  // Interface methods
  merkle_hash child_hash (u_int i) {
    assert (i < 64);
    return _child_hash[i];
  }
  merkle_node *child (u_int i);
  bool isleaf () const { return leaf; }
  void internal2leaf ();
  void internal2leaf (DB_TXN *t);
  void leaf2internal ();
  void leaf2internal (DB_TXN *t);
  void dump (u_int depth);

  operator str () const;

  merkle_node_bdb () : merkle_node (), depth (0), leaf (true), tree (NULL) {}
  merkle_node_bdb (const unsigned char *buf, size_t sz, merkle_tree_bdb *t);
  ~merkle_node_bdb ();
};

