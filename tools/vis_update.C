#include <gtk/gtk.h>
#include <gdk/gdk.h>

#include <chord.h>
#include <dns.h>
#include <gtk/gtk.h>
#include "id_utils.h"
#include "vis.h"

GtkWidget *total_nodes (NULL);
short interval (1000);

vec<chordID> get_queue;
ihash<chordID, f_node, &f_node::ID, &f_node::link, hashID> nodes;

extern bool accordion;

// f_node functions
f_node::f_node (const chord_node &n) :
  ID (n.x), host (n.r.hostname), hostname(""), port (n.r.port),
  vnode_num (n.vnode_num),
  fingers (NULL), predecessor (NULL), successors (NULL),
  selected (true), highlight (false)
{
  draw = check_get_state ();
  for (u_int i = 0; i < n.coords.size (); i++)
    coords.push_back (n.coords[i]);
  dnslookup ();
}

f_node::f_node (chordID i, str h, unsigned short p) :
    ID (i), host (h), port (p), vnode_num (0),
    fingers (NULL), predecessor (NULL), successors (NULL),
    selected (true), highlight (false)
{ 
  draw = check_get_state ();
  dnslookup ();
};

void
f_node::dnslookup (void)
{
  struct in_addr ar;
  inet_aton (host, &ar);
  dns_hostbyaddr (ar, wrap (this, &f_node::dnslookup_cb));
  hostname = host;
}
  
void
f_node::dnslookup_cb (ptr<hostent> he, int err) {
  if (err)
    warn << "dns lookup error\n";
  else
    hostname = he->h_name;
}

f_node::~f_node () { 
  if (fingers) delete fingers;
  if (predecessor) delete predecessor;
  if (successors) delete successors;
}

f_node *
add_node (const chord_node &n)
{
  f_node *nu = nodes[n.x];
  if (!nu) {
    warn << "added " << n << "\n";
    nu = New f_node (n);
    nodes.insert (nu);

    char nodess[1024];
    sprintf (nodess, "%zd nodes", nodes.size ());
    gtk_label_set_text (GTK_LABEL (total_nodes), nodess);
  }
  get_queue.push_back (n.x);
  return nu;
}


f_node *
add_node (str host, unsigned short port)
{
  chord_node n;
  n.x = make_chordID (host, port);
  n.r.hostname = host;
  n.r.port = port;
  n.vnode_num = 0; // Only for initial node.
  n.coords.clear ();
  f_node *nu = add_node (n);
  return nu;
}

void
get_cb (chordID next) 
{
  draw_ring ();
  if (get_queue.size ()) {
    chordID n = get_queue.pop_front ();
    f_node *nu = nodes[n];
    if (nu)
      update (nu);
  } else {
    f_node *node_next = nodes[next];
    if (node_next == NULL)
      node_next = nodes.first ();
    if (node_next) {
      update (node_next);
      node_next = nodes.next (node_next);
      if (node_next == NULL) 
	node_next = nodes.first ();
      next = node_next->ID;
    } // else no nodes, keep calling get_cb until there are some
  }

  delaycb (0, 1000*1000*interval, wrap (&get_cb, next));
}

// ----------------------

void
update (f_node *n)
{
  update_fingers (n);
  update_pred (n);
  update_succlist (n);
}

void
update () 
{
  if (simulated_input) return;
  f_node *n = nodes.first ();
  while (n) {
    update (n);
    n = nodes.next (n);
  }  
}

void
update_highlighted ()
{
  if (simulated_input) return;
  f_node *n = nodes.first ();
  while (n) {
    if (n->highlight)
      update (n);
    n = nodes.next (n);
  }
}

//----- update successors -----------------------------------------------------

void
update_succlist (f_node *nu)
{
  chordID n = nu->ID;
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (nu, chord_program_1, CHORDPROC_GETSUCC_EXT, &n, res,
	 wrap (&update_succ_got_succ, 
	       nu->ID, nu->host, nu->port, res));
}

void
update_succ_got_succ (chordID ID, str host, unsigned short port, 
		      chord_nodelistextres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    delete res;
    return;
  }
  f_node *nu = nodes[ID]; // callback shouldn't be called if not in list.

  if (nu->successors) delete nu->successors;
  nu->successors = res;
  
  for (unsigned int i=0; i < res->resok->nlist.size (); i++) {
    chord_node n = make_chord_node (res->resok->nlist[i].n);
    if (nodes[n.x] == NULL) 
      add_node (n);
  }
}

//----- update predecessor -------------------------------------------------
void
update_pred (f_node *nu)
{
  chordID n = nu->ID;
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (nu, chord_program_1, CHORDPROC_GETPRED_EXT, &n, res,
	 wrap (&update_pred_got_pred,
	       nu->ID, nu->host, nu->port, res));
}

void
update_pred_got_pred (chordID ID, str host, unsigned short port, 
		      chord_nodelistextres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    delete res;
    return;
  }
  f_node *nu = nodes[ID];

  if (nu->predecessor) delete nu->predecessor;
  nu->predecessor = res;

  if (res->resok->nlist.size()) {
    chord_node n = make_chord_node (res->resok->nlist[0].n);
    if (nodes[n.x] == NULL) 
      add_node (n);
  }
}

//----- update fingers -----------------------------------------------------
void
update_fingers (f_node *nu)
{
  chordID n = nu->ID;
  chord_nodelistextres *res = New chord_nodelistextres ();
  if (accordion) 
    doRPC (nu, accordion_program_1, ACCORDIONPROC_GETFINGERS_EXT, &n, res,
	 wrap (&update_fingers_got_fingers, 
	       nu->ID, nu->host, nu->port, res));
   else 
    doRPC (nu, fingers_program_1, FINGERSPROC_GETFINGERS_EXT, &n, res,
	 wrap (&update_fingers_got_fingers, 
	       nu->ID, nu->host, nu->port, res));
}

void
update_fingers_got_fingers (chordID ID, str host, unsigned short port, 
			    chord_nodelistextres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    delete res;
    return;
  }
  f_node *nu = nodes[ID];

  if (nu->fingers) delete nu->fingers;
  nu->fingers = res;

  for (unsigned int i=0; i < res->resok->nlist.size (); i++) {
    chord_node n = make_chord_node (res->resok->nlist[i].n);
    if (nodes[n.x] == NULL) 
      add_node (n);
  }
}

