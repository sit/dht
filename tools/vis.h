#include "chord_prot.h"
#include "fingers_prot.h"
#include "accordion_prot.h"
#include "transport_prot.h"

#include "id_utils.h"

#include <gtk/gtk.h>
#include <gdk/gdk.h>

#include <ihash.h>

struct f_node {
  chordID ID;
  vec<float> coords;

  str host, hostname;
  unsigned short port;
  unsigned short vnode_num;
  chord_nodelistextres *fingers;
  chord_nodelistextres *predecessor;
  chord_nodelistextres *successors;
  ihash_entry <f_node> link;
  unsigned int draw;
  bool selected;
  bool highlight;

  f_node (const chord_node &n);
  f_node (chordID i, str h, unsigned short p);
  void dnslookup (void);  
  void dnslookup_cb (ptr<hostent> he, int err);
  ~f_node ();
};

struct annotation {
  virtual void draw (bool ggeo, GtkWidget *drawing_area) = 0;
  virtual ~annotation () = 0;
};
struct arrow_annotation : public annotation {
  int xa, ya, xb, yb;
  arrow_annotation (int a, int b, int c, int d)
    : xa (a), ya (b), xb (c), yb (d) {};
  void draw (bool ggeo, GtkWidget *drawing_area);
};
struct arc_annotation : public annotation {
  chordID id_a, id_b;
  arc_annotation (chordID a, chordID b) : id_a (a), id_b (b) {};
  void draw (bool ggeo, GtkWidget *drawing_area);
};
extern vec<ptr<annotation> > annotations;

extern bool simulated_input;
extern ihash<chordID, f_node, &f_node::ID, &f_node::link, hashID> nodes;

void doRPC (f_node *nu, const rpc_program &prog,
	    int procno, const void *in, void *out, aclnt_cb cb);

void update_fingers (f_node *n);
void update_fingers_got_fingers (chordID ID, str host, unsigned short port, 
				 chord_nodelistextres *res, clnt_stat err);

void update_succlist (f_node *n);
void update_succ_got_succ (chordID ID, str host, unsigned short port, 
			   chord_nodelistextres *res, clnt_stat err);

void update_pred (f_node *n);
void update_pred_got_pred (chordID ID, str host, unsigned short port,
			   chord_nodelistextres *res, clnt_stat err);

f_node *add_node (const chord_node &n);
f_node *add_node (str host, unsigned short port);
void get_cb (chordID next);
void update ();
void update (f_node *n);
void update_highlighted ();

extern short interval; /* How frequently to update */
extern GtkWidget *total_nodes; /* A widget to display how many nodes exist */

unsigned int check_get_state (void);
void check_set_state (unsigned int newstate);
void ID_to_xy (chordID ID, int *x, int *y);

void draw_ring ();
void draw_arrow (int fromx, int fromy, 
		 int tox, int toy, GdkGC *draw_gc);
void draw_arc (chordID from, chordID to, GdkGC *draw_gc);
void draw_nothing_cb (GtkWidget *widget, gpointer data);

void setup_cmd (int p);
