#include "chord.h"
#include "gtk/gtk.h"
#include "gdk/gdk.h"
#include "math.h"
#include "rxx.h"
#include "async.h"
#include "chord_prot.h"
#include "debruijn_prot.h"
#include "prox_prot.h"
#include "fingers_prot.h"
#include "transport_prot.h"
#include "misc_utils.h"
#include "id_utils.h"
#include "dns.h"

#define WINX 700
#define WINY 700
#define PI 3.14159
#define TIMEOUT 10

#define NELEM(x)	(sizeof (x)/ sizeof ((x)[0]))

// Interesting things to draw and their handlers.
static const unsigned int DRAW_IMMED_SUCC = 1 << 0;
static const unsigned int DRAW_SUCC_LIST  = 1 << 1;
static const unsigned int DRAW_IMMED_PRED = 1 << 2;
static const unsigned int DRAW_DEBRUIJN   = 1 << 3;
static const unsigned int DRAW_FINGERS    = 1 << 4;
static const unsigned int DRAW_TOES       = 1 << 5;

struct handler_info {
  unsigned int flag;
  char *name;
  GtkWidget *widget;
  GtkSignalFunc handler;
};
void draw_toggle_cb (GtkWidget *widget, gpointer data);
// widgets will be initialized later, by initgraf.
static handler_info handlers[] = {
  { DRAW_IMMED_SUCC, "immed. succ", NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_SUCC_LIST,  "succ. list",  NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_DEBRUIJN,  "debruijn node",  NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_IMMED_PRED, "immed. pred", NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_FINGERS,    "fingers",     NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_TOES,       "neighbors",   NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) }
};

unsigned int check_get_state (void);
void check_set_state (unsigned int newstate);

/* GTK stuff */
static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GtkWidget *drawing_area_r = NULL;
static GtkWidget *drawing_area_g = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static GdkFont *courier10 = NULL;

static GtkWidget *lookup;
static GtkWidget *total_nodes;

static short interval = -1;
static int glevel = 1;
static char *color_file;
static bool drawids = false;
static bool simulated_input = false;

static GdkColor highlight_color;
static char *highlight = "cyan4"; // consistent with old presentations

static int xindex = 0;
static int yindex = 1;

static float  zoomx = 1.0;
static float  zoomy = 1.0;
static float centerx = 0.0;
static float centery = 0.0;

static bool ggeo = false;
static bool dual = false;

struct color_pair {
  GdkColor c;
  unsigned long lat;
};

vec<color_pair> lat_map;

char last_clicked[128] = "";

struct f_node {
  chordID ID;
  vec<float> coords;

  str host, hostname;
  unsigned short port;
  chord_nodelistextres *fingers;
  chord_nodeextres *predecessor;
  debruijn_res *debruijn;
  chord_nodelistextres *successors;
  chord_nodelistextres *toes;
  ihash_entry <f_node> link;
  unsigned int draw;
  bool selected;
  bool highlight;

  f_node (const chord_node &n) :
    ID (n.x), host (n.r.hostname), hostname(""), port (n.r.port),
    fingers (NULL), predecessor (NULL), debruijn (NULL),
    successors (NULL), toes (NULL),
    selected (true), highlight (false)
  {
    draw = check_get_state ();
    for (u_int i = 0; i < n.coords.size (); i++)
      coords.push_back (n.coords[i]);
    dnslookup();
  }

  f_node (chordID i, str h, unsigned short p) :
    ID (i), host (h), port (p), selected (true), highlight (false) { 
    draw = check_get_state ();
    fingers = NULL;
    predecessor = NULL;
    successors = NULL;
    toes = NULL;
    debruijn = NULL;
    dnslookup();
  };

  void dnslookup (void) {
    struct in_addr ar;
    inet_aton (host, &ar);
    dns_hostbyaddr (ar, wrap (this, &f_node::dnslookup_cb));
    hostname = host;
  }
  
  void dnslookup_cb (ptr<hostent> he, int err) {
    if (err)
      warn << "dns lookup error\n";
    else
      hostname = he->h_name;
  }

  ~f_node () { 
    if (fingers) delete fingers;
    if (predecessor) delete predecessor;
    if (successors) delete successors;
    if (toes) delete toes;
    if (debruijn) delete debruijn;
  };
};

// Highlighting lookups
static size_t search_step;
static chordID search_key; // 0 means no search in progress
static vec<f_node *> search_path;
static GdkColor search_color;

void recenter ();
void setup ();
ptr<aclnt> get_aclnt (str host, unsigned short port);

f_node *add_node (const chord_node &n);
void get_cb (chordID next);

void update_fingers (f_node *n);
void update_fingers_got_fingers (chordID ID, str host, unsigned short port, 
				 chord_nodelistextres *res, clnt_stat err);

void update_toes (f_node *nu);
void update_toes_got_toes (chordID ID, str host, unsigned short port, 
			   chord_nodelistextres *res, clnt_stat err);

void update_succlist (f_node *n);
void update_succ_got_succ (chordID ID, str host, unsigned short port, 
			   chord_nodelistextres *res, clnt_stat err);

void update_pred (f_node *n);
void update_pred_got_pred (chordID ID, str host, unsigned short port,
			   chord_nodeextres *res, clnt_stat err);

void update_debruijn (f_node *nu);
void update_debruijn_got_debruijn (chordID n, debruijn_res *res,
			     clnt_stat err);
void update ();
void initgraf ();
void init_color_list (char *filename);
void draw_arrow (int fromx, int fromy, 
		 int tox, int toy, GdkGC *draw_gc);
void draw_arc (chordID from, chordID to, GdkGC *draw_gc);

static gint configure_event (GtkWidget *widget, GdkEventConfigure *event);
static gint expose_event (GtkWidget *widget, GdkEventExpose *event);
static gint delete_event (GtkWidget *widget, GdkEvent *event, gpointer data);
static gint key_release_event (GtkWidget *widget,
			       GdkEventKey *event,
			       gpointer data);
static gint button_down_event (GtkWidget *widget,
			       GdkEventButton *event,
			       gpointer data);

void select_all_cb (GtkWidget *widget, gpointer data);
void select_none_cb (GtkWidget *widget, gpointer data);
void draw_nothing_cb (GtkWidget *widget, gpointer data);

void lookup_cb (GtkWidget *widget, gpointer data);
void lookup_complete_cb (chordID n, chord_nodelistres *res, clnt_stat err);
void quit_cb (GtkWidget *widget, gpointer data);
void redraw_cb (GtkWidget *widget, gpointer data);
void update_cb (GtkWidget *widget, gpointer data);
void zoom_in_cb (GtkWidget *widget, gpointer data);
void geo_cb (GtkWidget *widget, gpointer data);
void dump_cb (GtkWidget *widget, gpointer data);
void redraw();
void draw_ring ();
void ID_to_xy (chordID ID, int *x, int *y);
chordID xy_to_ID (int sx, int sy);
void xy_to_coord (int x, int y, float *cx, float *cy);
void ID_to_string (chordID ID, char *str);
double ID_to_angle (chordID ID);
void set_foreground_lat (unsigned long lat);
int main (int argc, char** argv);
void gtk_poll ();

vec<chordID> get_queue;
ihash<chordID, f_node, &f_node::ID, &f_node::link, hashID> nodes;
ptr<axprt_dgram> dgram_xprt;

void
setup () 
{
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";
}

void
update (f_node *n)
{
  update_fingers (n);
  update_pred (n);
  update_debruijn (n);
  update_succlist (n);
  update_toes (n);
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

ptr<aclnt>
get_aclnt (str host, unsigned short port)
{
  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, transport_program_1, 
			       (sockaddr *)&(saddr));

  return c;
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
    sprintf (nodess, "%d nodes", nodes.size ());
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
  n.coords.clear ();
  f_node *nu = add_node (n);
  return nu;
}

void
doRPCcb (chordID ID, xdrproc_t outproc, dorpc_res *res, void *out, aclnt_cb cb, clnt_stat err)
{
  f_node *nu = nodes[ID];

  if (!nu) return;
  // If we've already removed a node, then there's no reason to even
  // notify the cb of anything in this program.
  
  if (err || res->status == DORPC_UNKNOWNNODE) {
    if (!err) warn << "status: " << res->status << "\n";
    warn << "deleting " << ID << ":" << nu->host << "\n";
    nodes.remove (nu);
    char nodess[1024];
    sprintf (nodess, "%d nodes", nodes.size ());
    gtk_label_set_text (GTK_LABEL (total_nodes), nodess);
    delete nu;
    return;
  }

  // Don't have good results here, so just ignore it.
  if (res->status != DORPC_OK)
    return;
  
  nu->coords.clear ();
  for (unsigned int i = 0; i < res->resok->src_coords.size (); i++)
    nu->coords.push_back (((float)res->resok->src_coords[i]));

  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);
  if (!outproc (x.xdrp (), out)) {
    fatal << "failed to unmarshall result.\n";
    cb (RPC_CANTSEND);
  } else 
    cb (err);


}

void
doRPC (f_node *nu, const rpc_program &prog, int procno, const void *in, void *out, aclnt_cb cb)
{
  ptr<aclnt> c = get_aclnt (nu->host, nu->port);
  if (c == NULL) 
    fatal << "update_succlist: couldn't aclnt::alloc\n";

  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  arg->dest_id = nu->ID;
  arg->src_id = bigint (0);
  arg->src_vnode_num = 0;
  arg->progno = prog.progno;
  arg->procno = procno;
  
  //marshall the args ourself
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  xdrproc_t outproc = prog.tbl[procno].xdr_res;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), (void *)in))) {
    fatal << "failed to marshall args\n";
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    void *marshalled_args = suio_flatten (x.uio ());
    memcpy (arg->args.base (), marshalled_args, args_len);
    free (marshalled_args);

    dorpc_res *res = New dorpc_res (DORPC_OK);

    c->timedcall (TIMEOUT, TRANSPORTPROC_DORPC, 
		  arg, res, wrap (&doRPCcb, nu->ID, outproc, res, out, cb));
    
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
  chord_nodeextres *res = New chord_nodeextres ();
  doRPC (nu, chord_program_1, CHORDPROC_GETPRED_EXT, &n, res,
	 wrap (&update_pred_got_pred,
	       nu->ID, nu->host, nu->port, res));
}

void
update_pred_got_pred (chordID ID, str host, unsigned short port, 
		      chord_nodeextres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    delete res;
    return;
  }
  f_node *nu = nodes[ID];

  if (nu->predecessor) delete nu->predecessor;
  nu->predecessor = res;

  chord_node n = make_chord_node (res->resok->n);
  if (nodes[n.x] == NULL) 
    add_node (n);
}

//----- update debruijn finger -------------------------------------------------

void
update_debruijn (f_node *nu)
{
  return;
  chordID n = nu->ID;
  ptr<debruijn_arg> arg = New refcounted<debruijn_arg> ();
  arg->n = n;
  arg->x = n + 1;
  arg->i = n + 1;
  arg->upcall_prog = 0;
  
  debruijn_res *nres = New debruijn_res (CHORD_OK);
  doRPC (nu, debruijn_program_1, DEBRUIJNPROC_ROUTE, arg, nres, 
	 wrap (&update_debruijn_got_debruijn, n, nres));
}

void
update_debruijn_got_debruijn (chordID ID, debruijn_res *res, clnt_stat err)
{
  if (err  || res->status == CHORD_INRANGE) {
    delete res;
    return;
  }
  assert (res->status == CHORD_NOTINRANGE);
  f_node *nu = nodes[ID];

  if (nu->debruijn) delete nu->debruijn;
  nu->debruijn = res;

  chord_node n = make_chord_node (res->noderes->node);
  if (nodes[n.x] == NULL) 
    add_node (n);
}


//----- update fingers -----------------------------------------------------
void
update_fingers (f_node *nu)
{
  chordID n = nu->ID;
  chord_nodelistextres *res = New chord_nodelistextres ();
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


void
get_fingers (str file) 
{
  int i = 0;
  char s[1024];

  strbuf data = strbuf () << file2str (file);
  chord_nodelistextres *nfingers = NULL;
  chord_nodelistextres *ntoes = NULL;
  while (str line = suio_getline(data.tosuio ())) {
    f_node *n;
    switch (i) {
    case 0:
      {
	int ID = atoi (line);
	n = New f_node (bigint (ID) * (bigint(1) << (160-24)),
			file, 0);
	nodes.insert (n);
	nfingers = New chord_nodelistextres (CHORD_OK);
	ntoes = New chord_nodelistextres (CHORD_OK);
	n->fingers = nfingers;
	n->toes = ntoes;
      }
      break;
    case 1:
    case 2:
      {
	vec<int> ids;
	vec<int> lats;
	const char *cs = line.cstr ();
	strcpy (s, cs);
	char *start = s;
	char *end = s;
	while ((end = strchr(start, ' '))) {
	  *end = 0;
	  end++;
	  char *sid = start; 
	  char *latency = strchr(sid, '|');
	  *latency = 0;
	  latency++;
	  ids.push_back (atoi (sid));
	  lats.push_back (atoi (latency));
	  start = end;
	}
	if (i == 1) {
	  nfingers->resok->nlist.setsize (ids.size ());
	  for (unsigned int i = 0; i < ids.size (); i++) {
	    nfingers->resok->nlist[i].a_lat = lats[i] * 1000;
	  }
	} else {
	  ntoes->resok->nlist.setsize (ids.size ());
	  for (unsigned int i = 0; i < ids.size (); i++) {
	    ntoes->resok->nlist[i].a_lat = lats[i] * 1000;
	  }
	}
      }
      break;
    }
    i = (i + 1) % 3;
  }
  draw_ring ();
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
//----- update toes -----------------------------------------------------

void
update_toes (f_node *nu)
{
  prox_gettoes_arg n;
  n.level = glevel;
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (nu, prox_program_1, PROXPROC_GETTOES, &n, res,
	 wrap (&update_toes_got_toes, 
	       nu->ID, nu->host, nu->port, res));
}

void
update_toes_got_toes (chordID ID, str host, unsigned short port, 
		      chord_nodelistextres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    delete res;
    return;
  }
  f_node *nu = nodes[ID];

  if (nu->toes) delete nu->toes;
  nu->toes = res;
  
  for (unsigned int i=0; i < res->resok->nlist.size (); i++) {
    chord_node n = make_chord_node (res->resok->nlist[i].n);
    if (nodes[n.x] == NULL) 
      add_node (n);
  }

}

// ------- graphics and UI handlers -------------------------------------
void
initgraf ()
{
  courier10 = gdk_font_load ("-*-courier-*-r-*-*-*-*-*-*-*-*-*-*");

  window = gtk_window_new(GTK_WINDOW_TOPLEVEL);

  drawing_area = drawing_area_r = gtk_drawing_area_new();
  gtk_drawing_area_size ((GtkDrawingArea *)drawing_area_r, WINX, WINY);
  if (dual) {
    drawing_area_g = gtk_drawing_area_new();
    gtk_drawing_area_size ((GtkDrawingArea *)drawing_area_g, WINX, WINY);
  }

  GtkWidget *select_all = gtk_button_new_with_label ("Select All");
  GtkWidget *select_none = gtk_button_new_with_label ("Select None");
  GtkWidget *hsep1 = gtk_hseparator_new ();
  GtkWidget *draw_nothing = gtk_button_new_with_label ("Reset");
  GtkWidget *hsep2 = gtk_hseparator_new ();
  total_nodes = gtk_label_new ("                    ");
  for (size_t i = 0; i < NELEM (handlers); i++)
    handlers[i].widget = gtk_check_button_new_with_label (handlers[i].name);
  GtkWidget *hsep3 = gtk_hseparator_new ();
  lookup = gtk_button_new_with_label ("Visualize Lookup");
  GtkWidget *hsep4 = gtk_hseparator_new ();

  GtkWidget *in = gtk_button_new_with_label ("Recenter");
  GtkWidget *refresh = gtk_button_new_with_label ("Refresh All");
  GtkWidget *quit = gtk_button_new_with_label ("Quit");
  GtkWidget *sep = gtk_vseparator_new ();
  GtkWidget *geo = gtk_button_new_with_label ("Geo. View");
  GtkWidget *dump_to_file = gtk_button_new_with_label ("Save...");

  //organize things into boxes
  GtkWidget *vbox = gtk_vbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), select_all, FALSE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), select_none, FALSE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep1, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), draw_nothing, FALSE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep2, FALSE, TRUE, 0);
  for (size_t i = 0; i < NELEM (handlers); i++)
    gtk_box_pack_start (GTK_BOX (vbox), handlers[i].widget, FALSE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep3, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), lookup, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep4, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), total_nodes, FALSE, TRUE, 0);

  gtk_box_pack_end (GTK_BOX (vbox), quit, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), refresh, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), dump_to_file, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), geo, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), in, FALSE, FALSE, 0);

  GtkWidget *hbox = gtk_hbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (hbox), drawing_area_r, TRUE, FALSE, 0);
  if (dual)
    gtk_box_pack_start (GTK_BOX (hbox), drawing_area_g, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (hbox), sep, FALSE, TRUE, 5);
  gtk_box_pack_start (GTK_BOX (hbox), vbox, TRUE, FALSE, 0);

  gtk_container_add(GTK_CONTAINER (window), hbox);

  gtk_signal_connect_object (GTK_OBJECT (select_all), "clicked",
			       GTK_SIGNAL_FUNC (select_all_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (select_none), "clicked",
			       GTK_SIGNAL_FUNC (select_none_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (draw_nothing), "clicked",
			     GTK_SIGNAL_FUNC (draw_nothing_cb),
			     NULL);
  gtk_signal_connect_object (GTK_OBJECT (refresh), "clicked",
			       GTK_SIGNAL_FUNC (update_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (dump_to_file), "clicked",
			       GTK_SIGNAL_FUNC (dump_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (in), "clicked",
			       GTK_SIGNAL_FUNC (zoom_in_cb),
			       NULL);

  gtk_signal_connect_object (GTK_OBJECT (quit), "clicked",
			       GTK_SIGNAL_FUNC (quit_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (lookup), "clicked",
			     GTK_SIGNAL_FUNC (lookup_cb),
			     NULL);
  gtk_signal_connect_object (GTK_OBJECT (geo), "clicked",
			     GTK_SIGNAL_FUNC (geo_cb),
			     NULL);

  for (size_t i = 0; i < NELEM (handlers); i++) 
    gtk_signal_connect_object (GTK_OBJECT (handlers[i].widget), "toggled",
			       GTK_SIGNAL_FUNC (handlers[i].handler),
			       NULL);

  gtk_signal_connect (GTK_OBJECT (window), "delete_event",
		      GTK_SIGNAL_FUNC (delete_event), NULL);  
  gtk_signal_connect (GTK_OBJECT (window), "key_release_event",
		      GTK_SIGNAL_FUNC (key_release_event), NULL);
  gtk_widget_set_events (window, GDK_KEY_RELEASE_MASK);
  
  gtk_signal_connect (GTK_OBJECT (drawing_area_r), "expose_event",
		      (GtkSignalFunc) expose_event, NULL);
  gtk_signal_connect (GTK_OBJECT(drawing_area_r),"configure_event",
		      (GtkSignalFunc) configure_event, NULL);
  gtk_signal_connect (GTK_OBJECT (drawing_area_r), "button_press_event",
		      (GtkSignalFunc) button_down_event,
		      NULL);

  gtk_widget_set_events (drawing_area_r, GDK_EXPOSURE_MASK
			 | GDK_LEAVE_NOTIFY_MASK
			 | GDK_BUTTON_PRESS_MASK
			 | GDK_POINTER_MOTION_MASK
			 | GDK_POINTER_MOTION_HINT_MASK);

  gtk_widget_show (drawing_area_r);

  if (dual) {
    gtk_signal_connect (GTK_OBJECT (drawing_area_g), "expose_event",
			(GtkSignalFunc) expose_event, NULL);
    gtk_signal_connect (GTK_OBJECT(drawing_area_g),"configure_event",
			(GtkSignalFunc) configure_event, NULL);
    gtk_signal_connect (GTK_OBJECT (drawing_area_g), "button_press_event",
			(GtkSignalFunc) button_down_event,
			NULL);
    gtk_widget_set_events (drawing_area_g, GDK_EXPOSURE_MASK
			   | GDK_LEAVE_NOTIFY_MASK
			   | GDK_BUTTON_PRESS_MASK
			   | GDK_POINTER_MOTION_MASK
			   | GDK_POINTER_MOTION_HINT_MASK);
    gtk_widget_show (drawing_area_g);
  }

  gtk_widget_show (select_all);
  gtk_widget_show (select_none);
  gtk_widget_show (draw_nothing);
  for (size_t i = 0; i < NELEM (handlers); i++)
    gtk_widget_show (handlers[i].widget);
  gtk_widget_show (lookup);
  gtk_widget_show (total_nodes);
  gtk_widget_show (in);
  gtk_widget_show (dump_to_file);
  gtk_widget_show (geo);
  gtk_widget_show (refresh);
  gtk_widget_show (quit);
  gtk_widget_show (sep);
  gtk_widget_show (hsep1);
  gtk_widget_show (hsep2);
  gtk_widget_show (hsep3);
  gtk_widget_show (hsep4);
  gtk_widget_show (hbox);
  gtk_widget_show (vbox);
  gtk_widget_show (window);
  init_color_list (color_file);

  if (!gdk_color_parse (highlight, &highlight_color) ||
      !gdk_colormap_alloc_color (cmap, &highlight_color, FALSE, TRUE))
    fatal << "Couldn't allocate highlight color " << highlight << "\n";
  if (!gdk_color_parse ("green", &search_color) ||
      !gdk_colormap_alloc_color (cmap, &search_color, FALSE, TRUE))
    fatal << "Couldn't allocate search color maroon\n";
}

void
dump_cb (GtkWidget *widget, gpointer data)
{
  GdkPixbuf *pbuf = gdk_pixbuf_get_from_drawable (NULL,
						  pixmap,
						  NULL,
						  0,0,
						  0,0,
						  -1, -1);
  
  gdk_pixbuf_save (pbuf, "vis.jpeg", "jpeg", NULL, "quality", "100", NULL);
}

void
geo_cb (GtkWidget *widget, gpointer data)
{
  ggeo = !ggeo;
  redraw ();
}

void
zoom_in_cb (GtkWidget *widget, gpointer data)
{
  recenter ();
  redraw ();
}

void
redraw_cb (GtkWidget *widget, gpointer data)
{
  draw_ring ();
}

void 
quit_cb (GtkWidget *widget,
	 gpointer data)
{
  f_node *c = nodes.first ();
  f_node *n;
  while (c) {
    n = nodes.next (c);
    delete c;
    c = n;
  }
  gtk_exit (0);
}



unsigned int
check_get_state (void)
{
  unsigned int state = 0;
  for (size_t i = 0; i < NELEM (handlers); i++)
    if (gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (handlers[i].widget)))
      state |= handlers[i].flag;
  return state;
}

void
check_set_state (unsigned int newstate)
{
  // ONLY set the state of the buttons, do NOT actually toggle anything.
  for (size_t i = 0; i < NELEM (handlers); i++) {
    gtk_signal_handler_block_by_func (GTK_OBJECT (handlers[i].widget),
				      GTK_SIGNAL_FUNC (handlers[i].handler),
				      NULL);
    gtk_toggle_button_set_active (GTK_TOGGLE_BUTTON (handlers[i].widget),
				  (newstate & handlers[i].flag));
    gtk_signal_handler_unblock_by_func (GTK_OBJECT (handlers[i].widget),
					GTK_SIGNAL_FUNC (handlers[i].handler),
					NULL);
  }
}

void
draw_toggle_cb (GtkWidget *widget, gpointer data)
{

  // Set the state of all the selected nodes to match what the button says.
  bool active = false;
  unsigned int flag = 0;
  // xxx Shouldn't we be comparing "widget" to "check_immed_succ"?
  //     Empircally, no, this is what works. Weird.
  for (size_t i = 0; i < NELEM (handlers); i++)
    if (data == handlers[i].widget) {
      active = gtk_toggle_button_get_active
	(GTK_TOGGLE_BUTTON (handlers[i].widget));
      flag = handlers[i].flag;
      break;
    }
  
  f_node *n = nodes.first ();
  while (n) {
    if (n->selected) {
      if (active)
	n->draw |= flag; 
      else
	n->draw &= ~flag;
    }
    n = nodes.next (n);
  }
  draw_ring ();
}

chordID 
closestpredfinger (f_node *n, chordID &x)
{
  chordID p = n->ID;
  for (int i = n->fingers->resok->nlist.size () - 1; i >= 1; i--) {
    chordID t = make_chordID (n->fingers->resok->nlist[i].n);
    if (between (n->ID, x, t)) {
      p = t;
      return p;
    }
  }

  return p;
}

void
lookup_cb (GtkWidget *widget, gpointer data)
{
  // Find a random selected node, or just a random one if necessary.
  f_node *current_node = nodes.first ();
  while (current_node) {
    if (current_node->selected)
      break;
    current_node = nodes.next (current_node);
  }
  if (current_node == NULL)
    current_node = nodes.first ();
  
  // Clear selections and old search keys, etc.
  draw_nothing_cb (widget, data); // widget and data are ignored

  char block[8192]; 
  char id[sha1::hashsize];
  sha1_hash (id, block, 8192);
  mpz_set_rawmag_be (&search_key, id, sizeof (id));  // For big endian
  warnx << "Searching for " << search_key << " from "
	<< current_node->ID << "\n";
  current_node->selected = true;

  search_path.setsize (0);
  search_step = 0;
  search_path.push_back (current_node);

  chord_findarg fa;
  fa.x = search_key;
  chord_nodelistres *res = New chord_nodelistres ();
  doRPC (current_node, chord_program_1, CHORDPROC_FINDROUTE, &fa, res,
	 wrap (&lookup_complete_cb, current_node->ID, res));
  // XXX display a dialog box for progress...

#if 0	 
  // XXX one day this should make some sort of RPC into a chord node
  //     and call its find_route method.
  f_node *old_node = NULL;
  while (old_node != current_node) {
    old_node = current_node;
    search_path.push_back (current_node);
    chordID bestfinger = closestpredfinger (current_node, search_key);
    current_node = nodes[bestfinger];
  }
  current_node = nodes[current_node->successors->resok->nlist[1].n.x];
  search_path.push_back (current_node);
  
  warnx << "Found a path of length " << search_path.size () << "\n";
  for (size_t i = 0; i < search_path.size (); i++)
    warnx << "  " << search_path[i]->ID << "\n";
#endif /* 0 */
  draw_ring ();
}

void
lookup_complete_cb (chordID n, chord_nodelistres *res, clnt_stat err)
{
  if (err || res->status != CHORD_OK) {
    warnx << "WARNING! lookup to " << n << " of "
	  << search_key << " failed!\n";
    delete res;
    return;
  }

  for (size_t i = 0; i < res->resok->nlist.size (); i++) {
    chord_node n = make_chord_node (res->resok->nlist[i]);
    f_node *f = nodes[n.x];
    if (!f) {
      warnx << "WARNING! lookup includes a node we didn't know about!\n";
      f = add_node (n);
    }
    search_path.push_back (f);
  }
  warnx << "Found a path of length " << search_path.size () << "\n";
  for (size_t i = 0; i < search_path.size (); i++)
    warnx << "  " << search_path[i]->ID << "\n";
}

void
draw_search_progress ()
{
  GdkColor black;
  gdk_color_parse ("black", &black);
  gdk_gc_set_line_attributes (draw_gc, 5,
			      GDK_LINE_SOLID,
			      GDK_CAP_NOT_LAST,
			      GDK_JOIN_MITER);
  gdk_gc_set_foreground (draw_gc, &black);
  for (size_t i = 0; i < search_step; i++) {
    search_path[i]->selected = false;
    search_path[i]->highlight = true;
    search_path[i]->draw = 0;
  }

  for (size_t i = 1; i < search_step; i++)
    if (ggeo) {
      int tox, toy, fromx, fromy;
      ID_to_xy(search_path[0]->ID, &fromx, &fromy);
      ID_to_xy(search_path[i]->ID, &tox, &toy);
      draw_arrow (fromx, fromy, tox, toy, draw_gc);
    } else
      draw_arc (search_path[0]->ID, search_path[i]->ID, draw_gc);

  search_path[0]->selected = true;
  if (search_step != search_path.size ())
    search_path[search_step - 1]->draw = DRAW_FINGERS;

  gdk_gc_set_line_attributes (draw_gc, 3,
			      GDK_LINE_SOLID,
			      GDK_CAP_NOT_LAST,
			      GDK_JOIN_MITER);
}


void 
select_all_cb (GtkWidget *widget,
	       gpointer data) {
  
  f_node *n = nodes.first ();
  while (n) {
    n->selected = true;
    n = nodes.next (n);
  }
  draw_ring ();
}

void 
select_none_cb (GtkWidget *widget,
	        gpointer data) 
{
  f_node *n = nodes.first ();
  while (n) {
    n->selected = false;
    n = nodes.next (n);
  }
  draw_ring ();
}

void
update_cb (GtkWidget *widget,
	   gpointer data)
{
  update ();
}

void
draw_nothing_cb (GtkWidget *widget, gpointer data)
{
  f_node *n = nodes.first ();
  while (n) {
    n->selected = false;
    n->highlight = false;
    n->draw = 0;
    n = nodes.next (n);
  }
  check_set_state (0);
  search_key = 0;

  last_clicked[0] = 0;
  
  draw_ring ();
}

gint
key_release_event (GtkWidget *widget,
		   GdkEventKey *event,
		   gpointer data)
{
  // warnx << "key pressed " << event->keyval << "\n";
  switch (event->keyval) {
  case '1':
    xindex = 0;
    yindex = 1;
    break;
  case '2':
    xindex = 1;
    yindex = 2;
    break;
  case '3':
    xindex = 0;
    yindex = 2;
    break;
  case 'g':
    update_highlighted ();
    break;
  case 'n':
    {
      if (search_key == 0)
	break;

      search_step++;
      if (search_step > search_path.size ()) {
	search_key = 0;
	search_path.setsize (0);
	draw_nothing_cb (NULL, NULL);
	break;
      }
      draw_ring ();
      break;
    }
  case 'q':
  case 'Q':
    quit_cb (NULL, NULL);
    break;
  case 'z':
    zoomx = zoomx * 1.2;
    zoomy = zoomy * 1.2;
    break;
  case 'Z':
    zoomx = zoomx / 1.2;
    zoomy = zoomy / 1.2;
    break;
  default:
    break;
  }

  return (TRUE);
}

gint 
delete_event(GtkWidget *widget,
             GdkEvent  *event,
	     gpointer   data )
{
  gtk_exit (0);
  return (FALSE);
}


/* Create a new backing pixmap of the appropriate size */
static gint
configure_event (GtkWidget *widget, GdkEventConfigure *event)
{
  if (pixmap)
    gdk_pixmap_unref(pixmap);
  
  pixmap = gdk_pixmap_new(widget->window,
			  widget->allocation.width,
			  widget->allocation.height,
			  -1);
  gdk_draw_rectangle (pixmap,
		      widget->style->white_gc,
		      TRUE,
		      0, 0,
		      widget->allocation.width,
		      widget->allocation.height);
  
  return TRUE;
}

/* Redraw the screen from the backing pixmap */
static gint
expose_event (GtkWidget *widget, GdkEventExpose *event)
{
  gdk_draw_pixmap(widget->window,
		  widget->style->fg_gc[GTK_WIDGET_STATE (widget)],
		  pixmap,
		  event->area.x, event->area.y,
		  event->area.x, event->area.y,
		  event->area.width, event->area.height);
  
  return FALSE;
}

static gint button_down_event (GtkWidget *widget,
			       GdkEventButton *event,
			       gpointer data) 
{
  chordID ID = xy_to_ID ((int)event->x,(int)event->y);
  f_node *n = nodes[ID];
  assert (n);
  bool update_name = true;
  switch (event->button) {
  case 2:
    n->highlight = !n->highlight;
    break;
  case 3:
    if (ggeo)
      xy_to_coord ((int)event->x, (int)event->y, &centerx, &centery);
    update_name = false;
    break;
  default:
    n->selected = !n->selected;
    if (n->selected)
      check_set_state (n->draw);
    break;
  }

  if (update_name) {
    char hosts[512];
    strcpy (hosts, n->hostname);
    strcat (hosts, ":");
    strcat (hosts, ID.cstr ());
    strncpy (last_clicked, hosts, sizeof (last_clicked));
  }

  draw_ring ();
  return TRUE;
}

chordID
xy_to_ID (int sx, int sy)
{
  f_node *n = nodes.first ();
  int min = RAND_MAX;
  chordID closest = n->ID;
  while (n) {
    int x,y;
    ID_to_xy (n->ID, &x, &y);
    int dist = (sx - x)*(sx - x) + (sy - y)*(sy - y);
    if (dist < min) {
      closest = n->ID;
      min = dist;
    }
    n = nodes.next (n);
  }
  return closest;
}

void
init_color_list (char *filename)
{
  GdkColor c;
  color_pair p;
  draw_gc = NULL;
  draw_gc = gdk_gc_new (drawing_area->window);
  assert (draw_gc);
  
  gdk_gc_set_line_attributes (draw_gc, 3,
			      GDK_LINE_SOLID,
			      GDK_CAP_NOT_LAST,
			      GDK_JOIN_MITER);
  cmap = gdk_colormap_get_system ();

  FILE *cf = fopen (filename, "r");
  if (!cf) { 
    warn << "couldn't open " << filename << " using default color map\n";
    if (!gdk_color_parse ("red", &c) ||
	!gdk_colormap_alloc_color (cmap, &c, FALSE, TRUE))
      fatal << "couldn't get the color I wanted\n";
    p.c = c;
    p.lat = RAND_MAX;
    lat_map.push_back (p);
    return;
  }
  
  char color[1024];
  unsigned long lat;
  while (fscanf (cf, "%ld %s\n", &lat, color) == 2) {
    if (!gdk_color_parse (color, &c) ||
	!gdk_colormap_alloc_color (cmap, &c, FALSE, TRUE))
      fatal << "couldn't get the color I wanted\n";
    p.c = c;
    p.lat = lat * 1000; //convert from ms to microsec
    lat_map.push_back (p);
  }
  assert (lat_map.size () != 0);
}

void
draw_arc (chordID from, chordID to, GdkGC *draw_gc)
{

  double from_angle = ID_to_angle (from);
  double to_angle = ID_to_angle (to);
  double median = (from_angle + to_angle) / 2;
  if (to_angle < from_angle) median -= PI;
  double theta;
  if (to_angle > from_angle) theta = to_angle - from_angle;
  else theta = 2*PI - (from_angle - to_angle);

  double rad =  ((WINX)/2 - 10)*cos(theta/2)*0.8;

  int fromx, tox;
  int fromy, toy;
  ID_to_xy (from, &fromx, &fromy);
  ID_to_xy (to, &tox, &toy);

  if (theta < 0.1) {
    draw_arrow( (gint)fromx, (gint)fromy,
		(gint)tox,  (gint)toy, draw_gc);
    return;
  }

  double c1x, c2x;
  double c1y, c2y;

  c1x = 5 + (int)((WINX - 5)/2 + sin (median + 0.1)*rad);
  c1y = 5 + (int)((WINY - 5)/2 - cos (median + 0.1)*rad);
  c2x = 5 + (int)((WINX - 5)/2 + sin (median - 0.1)*rad);
  c2y = 5 + (int)((WINY - 5)/2 - cos (median - 0.1)*rad);

#ifdef DEBUG_BEZ
  gdk_draw_arc (pixmap,
		widget->style->black_gc,
		TRUE,
		(gint)c1x,(gint)c1y,
		4,4,
		(gint16)0, (gint16)64*360);

  gdk_draw_arc (pixmap,
		widget->style->black_gc,
		TRUE,
		(gint)c2x,(gint)c2y,
		4,4,
		(gint16)0, (gint16)64*360);
#endif

  int oldx = fromx;
  int oldy = fromy;
  for (float t=0.0; t < 0.99; t += 0.15) {
    float a = t;
    float b = 1 - t;
      
    float px = fromx*(b*b*b) + 3*c1x*b*b*a + 3*c2x*b*a*a 
      + tox*a*a*a;
    float py = fromy*(b*b*b) + 3*c1y*b*b*a + 3*c2y*b*a*a 
      + toy*a*a*a;
    
    gdk_draw_line (pixmap, 
		   draw_gc, 
		   (gint)oldx, (gint)oldy,
		   (gint)px,  (gint)py);
    oldx = (int)px;
    oldy = (int)py;
  }
  draw_arrow (oldx, oldy, tox, toy, draw_gc);

}


void
set_foreground_lat (unsigned long lat)
{
  GdkColor c = lat_map[0].c;
  unsigned int i = 0;

  // Each map entry indicates the high-limit latency for the entry's
  // color; we cap everything at the high-end.
  while (i < lat_map.size () && lat > lat_map[i].lat) 
    c = lat_map[i++].c;

  gdk_gc_set_foreground (draw_gc, &c);
}


void
draw_arrow (int fromx, int fromy, 
	    int tox, int toy, GdkGC *draw_gc)
{
  gdk_draw_line (pixmap,
		 draw_gc,
		 fromx,fromy,
		 tox,toy);

  float t = atan2 ((tox - fromx), (toy - fromy));
  float phi = PI/4 - t;
  float theta = PI/4 + t;
  float l = 10.0;

  float px = l*sin (phi) + tox;
  float py = -l*cos (phi) + toy;
  float p2y = toy - l*cos (theta);
  float p2x = tox - l*sin (theta);  

  GdkPoint head[4];
  head[0].x = (gint)px;
  head[0].y = (gint)py;
  head[1].x = (gint)tox;
  head[1].y = (gint)toy;
  head[2].x = (gint)p2x;
  head[2].y = (gint)p2y;

  gdk_draw_polygon (pixmap,
		    draw_gc,
		    true,
		    head,
		    3);
}

void
draw_ring ()
{

  if (dual) {
    if (ggeo)
      drawing_area = drawing_area_g;
    else
      drawing_area = drawing_area_r;
  }

  int x, y;
  GtkWidget *widget = drawing_area;

  GdkColor red;
  gdk_color_parse ("red", &red);

  gdk_draw_rectangle (pixmap,
		      widget->style->white_gc,
		      TRUE,
		      0,0,
		      WINX, WINY);
  gdk_draw_rectangle (pixmap,
		      widget->style->black_gc,
		      FALSE,
		      0,0,
		      WINX - 1, WINY - 1);

  if (search_key != 0 && (!ggeo || search_path.size () > 1)) {
    // Draw what we are looking for.
    ID_to_xy (search_key, &x, &y);
    gdk_gc_set_foreground (draw_gc, &search_color);
    gdk_draw_arc (pixmap, draw_gc, TRUE,
		  x - 8, y - 8,
		  16, 16,
		  (gint16) 0, (gint16) 64*360);
    if (search_step > 0)
      draw_search_progress (); // updates state for below and draws some arrows
  }

  f_node *n = nodes.first ();
  while (n) {
    int radius = 5;
    ID_to_xy (n->ID, &x, &y);
    GdkGC *thisgc = widget->style->black_gc;
    if (n->highlight) {
      radius = 7;
      gdk_gc_set_foreground (draw_gc, &highlight_color);
      thisgc = draw_gc;
    }

    if (!drawids) {
      gdk_draw_arc (pixmap,
		    thisgc,
		    TRUE,
		    x - radius, y - radius,
		    2*radius, 2*radius,
		    (gint16)0, (gint16)64*360);
      
      if (n->selected) {
	radius += 2;
	gdk_draw_arc (pixmap,
		      thisgc,
		      FALSE,
		      x - radius, y - radius,
		      2*radius, 2*radius,
		      (gint16)0, (gint16)64*360);
      }
    }

    if (drawids) {
      char IDs[48];
      ID_to_string (n->ID, IDs);
      int fudge = -10;
      if (x < WINX/2) fudge = 0;
      gdk_draw_string (pixmap,
		       courier10,
		       widget->style->black_gc,
		       x + fudge,y,
		       IDs);
    }

    if (!(ggeo && dual)) {

      if (n->successors && ((n->draw & DRAW_IMMED_SUCC) == DRAW_IMMED_SUCC) &&
	  n->successors->resok->nlist.size () > 1) {
	int a,b;
	set_foreground_lat (n->successors->resok->nlist[1].a_lat); 
	ID_to_xy (make_chordID (n->successors->resok->nlist[1].n), &a, &b);
	draw_arrow (x,y,a,b, draw_gc);
      }
      
      if (n->fingers && ((n->draw & DRAW_FINGERS) == DRAW_FINGERS)) {
	for (unsigned int i=1; i < n->fingers->resok->nlist.size (); i++) {
	  int a,b;
	  set_foreground_lat (n->fingers->resok->nlist[i].a_lat); 
	  ID_to_xy (make_chordID (n->fingers->resok->nlist[i].n), &a, &b);
	  draw_arrow (x,y,a,b, draw_gc);
	}
      }
      
      if (n->predecessor &&
	  ((n->draw & DRAW_IMMED_PRED) == DRAW_IMMED_PRED)) {
	int a,b;
	set_foreground_lat (n->predecessor->resok->a_lat); 
	ID_to_xy (make_chordID (n->predecessor->resok->n), &a, &b);
	draw_arrow (x,y,a,b, draw_gc);
      }
      
      if (n->debruijn &&
	  ((n->draw & DRAW_DEBRUIJN) == DRAW_DEBRUIJN)) {
	int a,b;
	set_foreground_lat (1); 
	ID_to_xy (make_chordID (n->debruijn->noderes->node), &a, &b);
	draw_arrow (x,y,a,b, draw_gc);
      }
      
      if (n->successors && ((n->draw & DRAW_SUCC_LIST) == DRAW_SUCC_LIST)) {
	for (unsigned int i=1; i < n->successors->resok->nlist.size (); i++) {
	  draw_arc (n->ID, make_chordID (n->successors->resok->nlist[i].n),
		    drawing_area->style->black_gc);
	}
      }
      
      if (n->toes && ((n->draw & DRAW_TOES) == DRAW_TOES)) {
	for (unsigned int i=0; i < n->toes->resok->nlist.size (); i++) {
	  int a,b;
	  ID_to_xy (make_chordID (n->toes->resok->nlist[i].n), &a, &b);
	  set_foreground_lat (n->toes->resok->nlist[i].a_lat); 
	  draw_arrow (x,y,a,b,draw_gc);
	}
      }
    }
    n = nodes.next (n);
  }
  
  gdk_draw_string (pixmap, courier10,
		   widget->style->black_gc,
		   15, 20,
		   last_clicked);
  
  redraw ();

  if(dual && !ggeo) {
    ggeo = true;
    draw_ring ();
  } else if (dual) {
    ggeo = false;
  }
}

void
ID_to_string (chordID ID, char *str) 
{
  bigint little = (ID >> 144);
  unsigned long z = little.getui ();
  sprintf(str, "%lx", z);
}

double 
ID_to_angle (chordID ID)
{
  bigint little = (ID >> 144);
  int z = little.getsi (); 
  return (z/65535.0) * 2 * 3.14159;
}

void
recenter ()
{
  f_node *n = nodes.first ();

  float minx=RAND_MAX, miny=RAND_MAX;
  float maxx=-RAND_MAX, maxy=-RAND_MAX;

  while (n) {
    if (n->coords.size () > 0) {
      float x = n->coords[xindex];
      float y = n->coords[yindex];
      minx = (x < minx) ? x : minx;
      miny = (y < miny) ? y : miny;
      maxx = (x > maxx) ? x : maxx;
      maxy = (y > maxy) ? y : maxy;
    }
    n = nodes.next (n);
  }

  centerx = (maxx + minx)/2.0;
  centery = (maxy + miny)/2.0;
  
  zoomx = maxx - minx;
  zoomy = maxy - miny;

  zoomx *= 1.3;
  zoomy *= 1.3;

  if (zoomx > zoomy) zoomy = zoomx;
  if (zoomy > zoomx) zoomx = zoomy;
}

void
xy_to_coord (int x, int y, float *cx, float *cy)
{
  *cx = (x - WINX/2)*zoomx/WINX + centerx;
  *cy = (y - WINY/2)*zoomy/WINY + centery;
}

void
ID_to_xy (chordID ID, int *x, int *y)
{
 
  f_node *f = nodes[ID];
  
  if (ggeo) {
    if (!f && search_path.size () > 0) // assume it is a lookup target
      f = nodes[search_path[search_path.size () - 1]->ID];
    if (f && f->coords.size () > 0) {
      *x = (int)(WINX/2 + ((f->coords[xindex] - centerx)/zoomx)*WINX);
      *y = (int)(WINY/2 + ((f->coords[yindex] - centery)/zoomy)*WINY);
    } else {
      if (f) warn << f->ID << " no coords? what gives\n";
      *x = WINX/2; 
      *y = WINY/2;
    }
  } else {

    double angle = ID_to_angle (ID);
    double radius = (WINX - 60)/2;
    
    *x = (int)(WINX/2 + sin (angle)*radius);
    *y = (int)(WINY/2 - cos (angle)*radius);
  }
}

void
redraw() 
{
  GdkRectangle update_rect;
  
  update_rect.x = 0;
  update_rect.y = 0;
  update_rect.width = WINX;
  update_rect.height = WINY;

  gtk_widget_draw( drawing_area, &update_rect);
}

void 
usage ()
{
  fatal << "vis [gtk options] -j <IP in dotted decimal>:<port> [-a delay]\n";
}

int
main (int argc, char** argv) 
{

  setprogname (argv[0]);
  sfsconst_init ();
  random_init ();

  setup ();
  gtk_init (&argc, &argv);

  str host = "not set";
  str sim_file = "network";
  unsigned short port = 0;
  interval = 100;
  color_file = ".viscolors";

  int ch;
  while ((ch = getopt (argc, argv, "h:j:a:l:f:is:d")) != -1) {
    switch (ch) {
    case 's':
      {
	simulated_input = true;
	sim_file = optarg;
	host = "simulated";
	break;
      }
    case 'j': 
      {
	char *bs_port = strchr(optarg, ':');
	if (!bs_port) usage ();
	*bs_port = 0;
	bs_port++;
	if (inet_addr (optarg) == INADDR_NONE) {
	  //yep, this blocks
	  struct hostent *h = gethostbyname (optarg);
	  if (!h) {
	    warn << "Invalid address or hostname: " << optarg << "\n";
	    usage ();
	  }
	  struct in_addr *ptr = (struct in_addr *)h->h_addr;
	  host = inet_ntoa (*ptr);
	} else
	  host = optarg;

	port = atoi (bs_port);

	break;
      }
    case 'a':
      {
	interval = atoi (optarg);
      }
      break;
    case 'l':
      {
	glevel = atoi (optarg);
	break;
      }
    case 'f':
      {
	color_file = optarg;
      }
      break;
    case 'i':
      {
	drawids = true;
      }
      break;
    case 'd':
      {
	dual = true;
      }
      break;
    case 'h':
      highlight = optarg;
      break;
  default:
    usage ();
    }
  };

  if (host == "not set")
    usage ();

  initgraf ();

  if (!simulated_input) {
    add_node (host, port);
    get_cb (NULL);
  } else
    get_fingers (sim_file);

  gtk_poll ();
  amain ();
}

void
gtk_poll () 
{
  // We never call main_loop so there's no reason to
  // ever use gtk_main_quit. Thus we don't care about
  // this return value. But we don't want this to block.
  (void) gtk_main_iteration_do (false);
  delaycb (0, 5000000, wrap (&gtk_poll));
}
