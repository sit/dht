/**
 * This program analyzes the keyauxdb files of all the vnodes of a
 * Chord ring, and outputs stats detailing how many keys each node is
 * missing in its local database, compared to all the keys that exist
 * in the network within the node's scope.
 *
 * You need to have a kdb file for each node in the ring, and you to
 * name them as follows: <node_id>.<type>.kdb.db.  Thus, the noauth
 * kdb for node fbc193dd04c4b653b878969a859b5d868a369b76 would be
 * called fbc193dd04c4b653b878969a859b5d868a369b76.n.kdb.db.
 *
 * To run it, simply run "./kdb_stats *.n.kdb.db" in the directory
 * with all the kdb files, which will produce the stats for all the
 * noauth side of things.
 *
 * The program will output (at its end) stats for each node of the form:
 *   
 * <node_id> <num_keys_in_scope> <num_keys_missing> <num_batches_of_missing_keys> <first_key_in_range_missing> <last_key_in_range_missing>
 **/

#include <async.h>
#include <itree.h>
#include <qhash.h>
#include <rxx.h>
#include <keyauxdb.h>
#include <id_utils.h>

struct node_rec {
  int pos;
  node_rec *next;
};

struct key_rec {
  chordID key;
  node_rec *nodes;
  itree_entry<key_rec> next;
};

struct node_stats {
  uint num_in_scope;
  uint num_missing;
  uint num_batch;
  bool last_missing;
  bool at_start;
  bool last_key_in_range;
};

typedef itree<chordID, key_rec, &key_rec::key, &key_rec::next> keytree;

//static const int NPREDS = 12;
static const int NPREDS = 2;

int main (int argc, char **argv) {

  if (argc <= 1) {
    warn << "Usage: kdb_stats kdbfiles\n";
    exit (0);
  }

  int num_nodes = argc-1;

  keytree sorted_nodes;
  keytree global_keys;
  keyauxdb **kdb = (keyauxdb **) malloc( sizeof(keyauxdb*)*num_nodes );
  vec<chordID> nodes;
  vec<str> nodes_str;

  for( int i = 0; i < num_nodes; i++ ) {
    str filename = argv[i+1];
    kdb[i] = New keyauxdb(filename);

    vec<str> parts;
    static const rxx slashy_rx( "\\/" );
    uint num_parts = split( &parts, slashy_rx, filename );
    rxx nodeid_rx( "(\\w+)\\.[cn]\\.kdb\\.db" );
    if( nodeid_rx.search( parts[num_parts-1] ) ) {
      chordID x;
      str2chordID( nodeid_rx[1], x );
      nodes.push_back(x);
      nodes_str.push_back(nodeid_rx[1]);
      key_rec *krec = New key_rec();
      krec->key = x;
      sorted_nodes.insert(krec);
    } else {
      fatal << "Could parse this file name: " << filename << ", " 
	    << parts[num_parts-1] << "\n";
    }

  }

  // now let's get us a sorted list of uniq-ified keys
  // (both global and per-node)
  uint num_keys = 0;
  uint num_keys_uniq = 0;
  for( int i = 0; i < num_nodes; i++ ) {
    keyauxdb *k = kdb[i];
    uint avail;
    uint at_a_time = 100000;
    const keyaux_t *keys;
    uint recno = 0;
    while( (keys = k->getkeys(recno, at_a_time, &avail)) && avail > 0 ) {
      for( uint j = 0; j < avail; j++ ) {
	key_rec *krec = New key_rec();
	krec->nodes = NULL;
	uint aux;
	keyaux_unmarshall( &(keys[j]), &krec->key, &aux );
	if( global_keys[krec->key] == NULL ) {
	  num_keys_uniq++;
	  global_keys.insert(krec);
	} else {
	  chordID k = krec->key;
	  delete krec;
	  krec = global_keys[k];
	}
	node_rec *n = New node_rec();
	n->pos = i;
	n->next = krec->nodes;
	krec->nodes = n;
	num_keys++;
      }
      recno += avail;
    }
    warn << "... " << num_keys_uniq << " " << num_keys << "\n";
    delete k;
    kdb[i] = NULL;
  }

  warn << "num_keys " << num_keys << ", num_keys_uniq " 
       << num_keys_uniq << "\n";

  // need the last one
  key_rec *last = NULL;
  key_rec *next = sorted_nodes.first();
  while( next != NULL ) {
    warn << next->key << " ";
    last = next;
    next = sorted_nodes.next(last);
  }
  warn << "\n";

  qhash<str, key_rec *> preds;
  qhash<str, node_stats *> stats;

  for( int i = 0; i < num_nodes; i++ ) {
    chordID curr_node = nodes[i];
    // find the nth predecessor
    int j = 1;
    key_rec *pred = sorted_nodes[curr_node];
    while( j < NPREDS ) {
      pred = sorted_nodes.prev(pred);
      if( pred == NULL ) {
	pred = last;
      }
      j++;
    }
    warn << "The last predecessor for " << curr_node << " is " 
	 << pred->key << "\n";

    preds.insert( nodes_str[i], pred );
    node_stats *s = New node_stats();
    s->num_in_scope = 0;
    s->num_missing = 0;
    s->num_batch = 0;
    s->last_missing = false;
    s->last_key_in_range = true;
    stats.insert( nodes_str[i], s );
  }

  key_rec *curr_key = global_keys.first();
  uint x = 0;
  while( curr_key != NULL ) {
    x++;
    for( int i = 0; i < num_nodes; i++ ) {
      chordID curr_node = nodes[i];
      str curr_node_str = nodes_str[i];
      node_stats *s = *(stats[curr_node_str]);
      // for each node, see if it belongs on the node, and if so, is it there?
      if( betweenrightincl( (*(preds[curr_node_str]))->key, curr_node, 
			    curr_key->key ) ) {
	s->num_in_scope++;
	bool found = false;
	node_rec *n = curr_key->nodes;
	while( n != NULL ) {
	  if( n->pos == i ) {
	    found = true;
	    break;
	  } else {
	    n = n->next;
	  }
	}
	if( !found ) {
	  s->num_missing++;
	  if( !s->last_missing ) {
	    s->num_batch++;
	    s->last_missing = true;
	  }
	} else {
	  s->last_missing = false;
	}

	// when this transitions from false to true, you've found the
	// beginning of the node's range
	if( !s->last_key_in_range ) {
	  s->last_key_in_range = true;
	  s->at_start = !found;
	}

      } else {
	s->last_key_in_range = false;
      }
    }
    curr_key = global_keys.next(curr_key);
  }

  for( int i = 0; i < num_nodes; i++ ) {
    node_stats *s = *(stats[nodes_str[i]]);
    warn << nodes[i] << " " << s->num_in_scope << " " 
	 << s->num_missing << " " << s->num_batch << " " << s->at_start
	 << " " << s->last_missing << "\n";
  }

}
