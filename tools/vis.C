#include <gtk/gtk.h>
#include <gdk/gdk.h>

#include "chord.h"
#include "math.h"
#include "rxx.h"
#include "async.h"
#include "misc_utils.h"
#include "vis.h"

#define WINX 700
#define WINY 700
#define PI 3.14159
#define TIMEOUT 10

#define NELEM(x)	(sizeof (x)/ sizeof ((x)[0]))

bool accordion = false;

// Interesting things to draw and their handlers.
static const unsigned int DRAW_IMMED_SUCC = 1 << 0;
static const unsigned int DRAW_SUCC_LIST  = 1 << 1;
static const unsigned int DRAW_IMMED_PRED = 1 << 2;
// static const unsigned int DRAW_DEBRUIJN   = 1 << 3;
static const unsigned int DRAW_FINGERS    = 1 << 4;
// static const unsigned int DRAW_TOES       = 1 << 5;
static const unsigned int DRAW_TOP_FINGERS       = 1 << 6;
static const unsigned int DRAW_PRED_LIST  = 1 << 7;

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
  { DRAW_PRED_LIST,  "pred. list",  NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_IMMED_PRED, "immed. pred", NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_FINGERS,    "fingers",     NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_TOP_FINGERS,    "top fingers",     NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) }
};

/* GTK stuff */
static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GtkWidget *drawing_area_r = NULL;
static GtkWidget *drawing_area_g = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static GdkFont *courier10 = NULL;

static int glevel = 1;
static char *color_file;
static bool drawids = false;
bool simulated_input (false);

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
static GdkColor search_color;

void recenter ();
void setup ();
ptr<aclnt> get_aclnt (str host, unsigned short port);

void initgraf ();
void init_color_list (char *filename);

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

void quit_cb (GtkWidget *widget, gpointer data);
void redraw_cb (GtkWidget *widget, gpointer data);
void update_cb (GtkWidget *widget, gpointer data);
void zoom_in_cb (GtkWidget *widget, gpointer data);
void geo_cb (GtkWidget *widget, gpointer data);
void dump_cb (GtkWidget *widget, gpointer data);
void redraw();
chordID xy_to_ID (int sx, int sy);
void xy_to_coord (int x, int y, float *cx, float *cy);
void ID_to_string (chordID ID, char *str);
double ID_to_angle (chordID ID);
void set_foreground_lat (unsigned long lat);

void gtk_poll ();

ptr<axprt_dgram> dgram_xprt;


void
setup () 
{
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";
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


void
doRPCcb (chordID ID, xdrproc_t outproc, dorpc_res *res, void *out, aclnt_cb cb, clnt_stat err)
{
  f_node *nu = nodes[ID];

  // If we've already removed a node, then there's no reason to even
  // notify the cb of anything in this program.
  if (!nu) {
    delete res;
    return;
  }
  
  if (err || res->status == DORPC_UNKNOWNNODE) {
    if (!err) warn << "status: " << res->status << "\n";
    warn << "deleting " << ID << ":" << nu->host << "\n";
    nodes.remove (nu);
    char nodess[1024];
    sprintf (nodess, "%zd nodes", nodes.size ());
    gtk_label_set_text (GTK_LABEL (total_nodes), nodess);
    delete nu;
    delete res;
    return;
  }

  // Don't have good results here, so just ignore it.
  if (res->status != DORPC_OK) {
    delete res;
    return;
  }
  
  nu->coords.clear ();
  for (unsigned int i = 0; i < 3; i++)
    nu->coords.push_back (((float)res->resok->src.coords[i]));

  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);
  if (!outproc (x.xdrp (), out)) {
    fatal << "failed to unmarshall result.\n";
    cb (RPC_CANTSEND);
  } else 
    cb (err);

  delete res;
}

void
doRPC (f_node *nu, const rpc_program &prog,
       int procno, const void *in, void *out, aclnt_cb cb)
{
  ptr<aclnt> c = get_aclnt (nu->host, nu->port);
  if (c == NULL) 
    fatal << "doRPC: couldn't aclnt::alloc\n";

  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  struct sockaddr_in saddr;
  bzero(&saddr, sizeof (sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (nu->host.cstr (), &saddr.sin_addr);

  arg->dest.machine_order_ipv4_addr = ntohl (saddr.sin_addr.s_addr);
  arg->dest.machine_order_port_vnnum = (nu->port << 16) | nu->vnode_num;
  //leave coords as random.
  bzero (&arg->src, sizeof (arg->src));

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
    x.uio ()->copyout (arg->args.base ());

    dorpc_res *res = New dorpc_res (DORPC_OK);

    c->timedcall (TIMEOUT, TRANSPORTPROC_DORPC, 
		  arg, res, wrap (&doRPCcb, nu->ID, outproc, res, out, cb));
    
  }
}  


void
get_fingers (str file) 
{
  int i = 0;
  char s[1024];

  strbuf data = strbuf () << file2str (file);
  chord_nodelistextres *nfingers = NULL;
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
	n->fingers = nfingers;
      }
      break;
    case 1:
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
	nfingers->resok->nlist.setsize (ids.size ());
	for (unsigned int i = 0; i < ids.size (); i++) {
	  nfingers->resok->nlist[i].a_lat = lats[i] * 1000;
	}
      }
      break;
    }
    i = (i + 1) % 2;
  }
  draw_ring ();
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
  
  gdk_pixbuf_save (pbuf, "vis.jpeg", "jpeg", NULL, "quality", "100", (void *) NULL);
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
  // search_key = 0;

  last_clicked[0] = 0;
  annotations.clear ();
  
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

      if (n->fingers && ((n->draw & DRAW_TOP_FINGERS) == DRAW_TOP_FINGERS)) {
	unsigned int i=n->fingers->resok->nlist.size () - 1;
	int a,b;
	set_foreground_lat (n->fingers->resok->nlist[i].a_lat); 
	ID_to_xy (make_chordID (n->fingers->resok->nlist[i].n), &a, &b);
	draw_arrow (x,y,a,b, draw_gc);
      }

      
      if (n->predecessor &&
	  ((n->draw & DRAW_IMMED_PRED) == DRAW_IMMED_PRED) &&
	  n->predecessor->resok->nlist.size () > 1) {
	int a,b;
	set_foreground_lat (n->predecessor->resok->nlist[1].a_lat); 
	ID_to_xy (make_chordID (n->predecessor->resok->nlist[1].n), &a, &b);
	draw_arrow (x,y,a,b, draw_gc);
      }
      
      if (n->successors && ((n->draw & DRAW_SUCC_LIST) == DRAW_SUCC_LIST)) {
	for (unsigned int i=1; i < n->successors->resok->nlist.size (); i++) {
	  draw_arc (n->ID, make_chordID (n->successors->resok->nlist[i].n),
		    drawing_area->style->black_gc);
	}
      }
      
      if (n->predecessor && ((n->draw & DRAW_PRED_LIST) == DRAW_PRED_LIST)) {
	for (unsigned int i=1; i < n->predecessor->resok->nlist.size (); i++) {
	  draw_arc (n->ID, make_chordID (n->predecessor->resok->nlist[i].n),
		    drawing_area->style->black_gc);
	}
      }
    }
    n = nodes.next (n);
  }

  for (size_t i = 0; i < annotations.size (); i++) {
    annotations[i]->draw (ggeo, drawing_area);
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
  fatal << "vis [gtk options] -j <IP in dotted decimal>:<port> [-m accordion] [-a delay]\n";
}

int
main (int argc, char** argv) 
{

  setprogname (argv[0]);
  random_init ();

  setup ();
  gtk_init (&argc, &argv);

  str host = "not set";
  str sim_file = "network";
  int cmd_port = 0;
  unsigned short port = 0;
  color_file = ".viscolors";

  int ch;
  while ((ch = getopt (argc, argv, "c:h:j:a:l:f:is:dm")) != -1) {
    switch (ch) {
    case 'c':
      {
	cmd_port = atoi (optarg);
	break;
      }
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
    case 'm':
      accordion = true;
      break;
  default:
    usage ();
    }
  };

  if (host == "not set")
    usage ();

  warn << " vis in " << (accordion? "ACCORDION" : "FINGER") << " mode\n";

  initgraf ();

  if (cmd_port > 0) {
    setup_cmd (cmd_port);
  }

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
