#include "chord.h"
#include "gtk/gtk.h"
#include "gdk/gdk.h"
#include "math.h"

#define WINX 600
#define WINY 600
#define PI 3.14159
#define TIMEOUT 10

/* GTK stuff */
static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static short interval = -1;
static GtkWidget *check_fingers;
static GtkWidget *check_immed_succ;
static GtkWidget *check_succ_list;
static GtkWidget *check_neighbors;
static int glevel = 1;
static char *color_file;
static bool drawids = false;

struct color_pair {
  GdkColor c;
  unsigned long lat;
};

vec<color_pair> lat_map;

struct f_node {
  chordID ID;
  str host;
  short port;
  chord_getfingers_ext_res *res;
  chord_getsucc_ext_res *ressucc;
  chord_gettoes_res *restoes;
  ihash_entry <f_node> link;
  bool draw;

  f_node (chordID i, str h, short p) :
    ID (i), host (h), port (p), draw (true) { 
    res = NULL; 
    ressucc = NULL; 
    restoes = NULL;
  };
  ~f_node () { 
    if (res) delete res; 
    if (ressucc) delete ressucc; 
    if (restoes) delete restoes;
  };
};

void setup ();
ptr<aclnt> get_aclnt (str host, short port);
void get_fingers (str host, short port);
void get_fingers (chordID ID, str host, short port);
void get_fingers_got_fingers (chordID ID, str host, short port, 
			      chord_getfingers_ext_res *res,
			      clnt_stat err);
void add_fingers (chordID ID, str host, short port, chord_getfingers_ext_res *res);
void update_fingers (f_node *n);
void update_fingers_got_fingers (chordID ID, str host, short port, 
				 chord_getfingers_ext_res *res,
				 clnt_stat err);

void update_toes (f_node *nu);
void update_toes_got_toes (chordID ID, str host, short port, 
			   chord_gettoes_res *res, clnt_stat err);

void get_succ (str host, short port);
void get_succ (chordID ID, str host, short port);
void get_succ_got_succ (chordID ID, str host, short port, 
			      chord_getsucc_ext_res *res,
			      clnt_stat err);
void add_succ (chordID ID, str host, short port, chord_getsucc_ext_res *res);
void update_succlist (f_node *n);
void update_succ_got_succ (chordID ID, str host, short port, 
				 chord_getsucc_ext_res *res,
				 clnt_stat err);
void update ();
void initgraf ();
void init_color_list (char *filename);
void draw_arrow (int fromx, int fromy, 
		 int tox, int toy, GdkGC *draw_gc);

static gint configure_event (GtkWidget *widget, GdkEventConfigure *event);
static gint expose_event (GtkWidget *widget, GdkEventExpose *event);
static gint delete_event(GtkWidget *widget, GdkEvent *event, gpointer data);
static gint button_down_event (GtkWidget *widget,
			       GdkEventButton *event,
			       gpointer data);
void draw_all_cb (GtkWidget *widget, gpointer data);
void draw_none_cb (GtkWidget *widget, gpointer data);
void quit_cb (GtkWidget *widget, gpointer data);
void redraw_cb (GtkWidget *widget, gpointer data);
void update_cb (GtkWidget *widget, gpointer data);
void redraw();
void draw_ring ();
void ID_to_xy (chordID ID, int *x, int *y);
chordID xy_to_ID (int sx, int sy);
void ID_to_string (chordID ID, char *str);
double ID_to_angle (chordID ID);
void set_foreground_lat (unsigned long lat);
int main (int argc, char** argv);
void gtk_poll ();

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
update () 
{
  f_node *n = nodes.first ();
  while (n) {
    update_fingers (n);
    update_succlist (n);
    n = nodes.next (n);
  }  

  if (interval > 0)
    delaycb (interval, 0, wrap (&update));
}

ptr<aclnt>
get_aclnt (str host, short port)
{
  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, chord_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

//----- update successors -----------------------------------------------------

void
update_succlist (f_node *nu)
{
  ptr<aclnt> c = get_aclnt (nu->host, nu->port);
  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";
  
  chord_vnode n;
  n.n = nu->ID;
  chord_getsucc_ext_res *res = New chord_getsucc_ext_res ();
  c->timedcall (TIMEOUT, CHORDPROC_GETSUCC_EXT, &n, res,
		wrap (&update_succ_got_succ, 
		      nu->ID, nu->host, nu->port, res));
}


void
update_succ_got_succ (chordID ID, str host, short port, 
			    chord_getsucc_ext_res *res, clnt_stat err)
{
  if (err || res->status) {
    warn << "(update succ) deleting " << ID << "\n";
    if (nodes[ID])
      nodes.remove (nodes[ID]);
    return;
  }

  f_node *nu = nodes[ID];
  delete nu->ressucc;
  nu->ressucc = res;

  for (unsigned int i=0; i < res->resok->succ.size (); i++) {
    if ( nodes[res->resok->succ[i].x] == NULL) 
      get_fingers (res->resok->succ[i].x, res->resok->succ[i].r.hostname,
		   res->resok->succ[i].r.port);
  }
}


void 
get_succ (str host, short port) 
{
  chordID ID = make_chordID (host, port);
  get_succ (ID, host, port);
}

void
get_succ (chordID ID, str host, short port) 
{
  ptr<aclnt> c = get_aclnt (host, port);
  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";

  chord_vnode n;
  n.n = ID;
  chord_getsucc_ext_res *res = New chord_getsucc_ext_res ();
  c->timedcall (TIMEOUT, CHORDPROC_GETSUCC_EXT, &n, res,
		wrap (&get_succ_got_succ, 
		      ID, host, port, res));
  
}

void
get_succ_got_succ (chordID ID, str host, short port, 
			 chord_getsucc_ext_res *res,
			 clnt_stat err) 
{
  if (err || res->status) {
    warn << "get succ failed, deleting: " << ID << "\n";
    if (nodes[ID])
      nodes.remove (nodes[ID]);
    draw_ring ();
  } else
    add_succ (ID, host, port, res);
}

void
add_succ (chordID ID, str host, short port, chord_getsucc_ext_res *res) 
{
  f_node *n = nodes[ID];

  if (n && n->ressucc) return;
  if (!n) {
    warn << "added " << ID << "\n";
    n = New f_node (ID, host, port);
    nodes.insert (n);
  }
  n->ressucc = res;
  for (unsigned int i=0; i < res->resok->succ.size (); i++) {
    if ( nodes[res->resok->succ[i].x] == NULL) 
      get_succ (res->resok->succ[i].x, res->resok->succ[i].r.hostname,
		   res->resok->succ[i].r.port);
  }
  draw_ring ();
}

//----- update fingers -----------------------------------------------------

void
update_fingers (f_node *nu)
{
  ptr<aclnt> c = get_aclnt (nu->host, nu->port);
  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";
  
  chord_vnode n;
  n.n = nu->ID;
  chord_getfingers_ext_res *res = New chord_getfingers_ext_res ();
  c->timedcall (TIMEOUT, CHORDPROC_GETFINGERS_EXT, &n, res,
		wrap (&update_fingers_got_fingers, 
		      nu->ID, nu->host, nu->port, res));
}

void
update_fingers_got_fingers (chordID ID, str host, short port, 
			    chord_getfingers_ext_res *res, clnt_stat err)
{
  if (err || res->status) {
    warn << "(update) deleting " << ID << "\n";
    if (nodes[ID])
      nodes.remove (nodes[ID]);
    return;
  }

  f_node *nu = nodes[ID];
  if (nu->res) delete nu->res;
  nu->res = res;

  update_toes (nu);
  for (unsigned int i=0; i < res->resok->fingers.size (); i++) {
    if ( nodes[res->resok->fingers[i].x] == NULL) 
      get_fingers (res->resok->fingers[i].x, res->resok->fingers[i].r.hostname,
		   res->resok->fingers[i].r.port);
  }
}

void 
get_fingers (str host, short port) 
{
  chordID ID = make_chordID (host, port);
  get_fingers (ID, host, port);
}

void
get_fingers (chordID ID, str host, short port) 
{
  ptr<aclnt> c = get_aclnt (host, port);
  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";

  chord_vnode n;
  n.n = ID;
  chord_getfingers_ext_res *res = New chord_getfingers_ext_res ();
  c->timedcall (TIMEOUT, CHORDPROC_GETFINGERS_EXT, &n, res,
		wrap (&get_fingers_got_fingers, 
		      ID, host, port, res));
  
}

void
get_fingers_got_fingers (chordID ID, str host, short port, 
			 chord_getfingers_ext_res *res,
			 clnt_stat err) 
{
  if (err || res->status) {
    warn << "get fingers failed, deleting: " << ID << "\n";
    if (nodes[ID])
      nodes.remove (nodes[ID]);
    draw_ring ();
  } else
    add_fingers (ID, host, port, res);
}

void
add_fingers (chordID ID, str host, short port, chord_getfingers_ext_res *res) 
{
  f_node *n = nodes[ID];

  if (n && n->res) return;
  if (!n) {
    warn << "added " << ID << "\n";
    n = New f_node (ID, host, port);
    nodes.insert (n);
  }
  n->res = res;
  for (unsigned int i=0; i < res->resok->fingers.size (); i++) {
    if ( nodes[res->resok->fingers[i].x] == NULL) 
      get_fingers (res->resok->fingers[i].x, res->resok->fingers[i].r.hostname,
		   res->resok->fingers[i].r.port);
  }
  update_toes (n);
  draw_ring ();
}


//----- update toes -----------------------------------------------------

void
update_toes (f_node *nu)
{
  ptr<aclnt> c = get_aclnt (nu->host, nu->port);
  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";
  
  chord_gettoes_arg n;
  n.v.n = nu->ID;
  n.level = glevel;
  chord_gettoes_res *res = New chord_gettoes_res ();
  c->timedcall (TIMEOUT, CHORDPROC_GETTOES, &n, res,
		wrap (&update_toes_got_toes, 
		      nu->ID, nu->host, nu->port, res));
}

void
update_toes_got_toes (chordID ID, str host, short port, 
		      chord_gettoes_res *res, clnt_stat err)
{
  if (err || res->status) {
    warn << "(update toes) deleting " << ID << "\n";
    if (nodes[ID])
      nodes.remove (nodes[ID]);
    return;
  }

  f_node *nu = nodes[ID];
  if (!nu) return;
  if (nu->restoes) delete nu->restoes;
  nu->restoes = res;
  draw_ring ();
}

void
initgraf ()
{
  window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
  drawing_area = gtk_drawing_area_new();
  gtk_drawing_area_size ((GtkDrawingArea *)drawing_area, WINX, WINY);

  GtkWidget *draw_all = gtk_button_new_with_label ("Show All");
  GtkWidget *draw_none = gtk_button_new_with_label ("Show None");
  check_fingers = gtk_check_button_new_with_label ("fingers");
  check_immed_succ = 
    gtk_check_button_new_with_label ("immed. succ.");
  check_succ_list = gtk_check_button_new_with_label ("succ. list");
  check_neighbors = gtk_check_button_new_with_label ("neighbors");
  GtkWidget *refresh = gtk_button_new_with_label ("Refresh");
  GtkWidget *quit = gtk_button_new_with_label ("Quit");
  GtkWidget *sep = gtk_vseparator_new ();

  //organize things into boxes
  GtkWidget *vbox = gtk_vbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), draw_all, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), check_fingers, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), check_immed_succ, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), check_succ_list, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), check_neighbors, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), draw_none, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), refresh, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), quit, TRUE, FALSE, 0);

  GtkWidget *hbox = gtk_hbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (hbox), drawing_area, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (hbox), sep, FALSE, TRUE, 5);
  gtk_box_pack_start (GTK_BOX (hbox), vbox, TRUE, FALSE, 0);

  gtk_container_add(GTK_CONTAINER (window), hbox);

  gtk_signal_connect_object (GTK_OBJECT (draw_all), "clicked",
			       GTK_SIGNAL_FUNC (draw_all_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (draw_none), "clicked",
			       GTK_SIGNAL_FUNC (draw_none_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (refresh), "clicked",
			       GTK_SIGNAL_FUNC (update_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (quit), "clicked",
			       GTK_SIGNAL_FUNC (quit_cb),
			       NULL);

  gtk_signal_connect_object (GTK_OBJECT (check_fingers), "toggled",
			     GTK_SIGNAL_FUNC (redraw_cb),
			     NULL);
  gtk_signal_connect_object (GTK_OBJECT (check_immed_succ), "toggled",
			     GTK_SIGNAL_FUNC (redraw_cb),
			     NULL);
  gtk_signal_connect_object (GTK_OBJECT (check_succ_list), "toggled",
			     GTK_SIGNAL_FUNC (redraw_cb),
			     NULL);
  gtk_signal_connect_object (GTK_OBJECT (check_neighbors), "toggled",
			     GTK_SIGNAL_FUNC (redraw_cb),
			     NULL);


  gtk_signal_connect (GTK_OBJECT (window), "delete_event",
		      GTK_SIGNAL_FUNC (delete_event), NULL);  
  gtk_signal_connect (GTK_OBJECT (drawing_area), "expose_event",
		      (GtkSignalFunc) expose_event, NULL);
  gtk_signal_connect (GTK_OBJECT(drawing_area),"configure_event",
		      (GtkSignalFunc) configure_event, NULL);
  gtk_signal_connect (GTK_OBJECT (drawing_area), "button_press_event",
		      (GtkSignalFunc) button_down_event,
		      NULL);

  gtk_widget_set_events (drawing_area, GDK_EXPOSURE_MASK
			 | GDK_LEAVE_NOTIFY_MASK
			 | GDK_BUTTON_PRESS_MASK
			 | GDK_POINTER_MOTION_MASK
			 | GDK_POINTER_MOTION_HINT_MASK);


  gtk_widget_show (drawing_area);
  gtk_widget_show (draw_all);
  gtk_widget_show (check_fingers);
  gtk_widget_show (check_succ_list);
  gtk_widget_show (check_neighbors);
  gtk_widget_show (check_immed_succ);
  gtk_widget_show (draw_none);
  gtk_widget_show (refresh);
  gtk_widget_show (quit);
  gtk_widget_show (sep);
  gtk_widget_show (hbox);
  gtk_widget_show (vbox);
  gtk_widget_show (window);

  init_color_list (color_file);
}

void
redraw_cb (GtkWidget *widget, gpointer data)
{
  draw_ring ();
}
void 
quit_cb (GtkWidget *widget,
	 gpointer data) {  
  gtk_exit (0);
}
void 
draw_all_cb (GtkWidget *widget,
	     gpointer data) {
  
  f_node *n = nodes.first ();
  while (n) {
    n->draw = true;
    n = nodes.next (n);
  }
  draw_ring ();
}

void 
draw_none_cb (GtkWidget *widget,
	      gpointer data) 
{
  f_node *n = nodes.first ();
  while (n) {
    n->draw = false;
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
  n->draw = !n->draw;
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
  draw_gc = gdk_gc_new (drawing_area->window);
  assert (draw_gc);
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
  while (fscanf (cf, "%ld %s\n", &lat, color) == 2) 
    {
      if (!gdk_color_parse (color, &c) ||
	  !gdk_colormap_alloc_color (cmap, &c, FALSE, TRUE))
	fatal << "couldn't get the color I wanted\n";
      p.c = c;
      p.lat = lat * 1000 * 100; //convert from ms
      lat_map.push_back (p);
    }
  

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
  for (float t=0.0; t < 0.99; t += 0.01) {
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
    /*
      int m1, m2;
      int w, h;
      gint16 a1;
      if (((x < a) && (y < b)) || ((x >= a) && (y < b))) {
      m1 = a;
      m2 = y;
      w = (x < m1) ? (m1 - x) : (x - m1);
      h = (b < m2) ? (m2 - b) : (b - m2);
      a1 = (x < a) ? 32 : 48;
      } else {
      m1 = x;
      m2 = b;
      w = (a < m1) ? (m1 - a) : (a - m1);
      h = (y < m2) ? (m2 - y) : (y - m2);
      a1 = (x < a ) ? 48 : 32;
      }
      int l = m1 - w;
      int t = m2 - h;
      a1 = (gint16) a1 * 360;
      gint16 a2 = (gint16) 16*360;
      gdk_draw_arc (pixmap, draw_gc, FALSE, l, t, 2*w, 2*h, a1, a2);
    */
}


void
set_foreground_lat (unsigned long lat)
{
  unsigned int i = 0; 
  while (i < lat_map.size () &&
	 lat > lat_map[i].lat) i++;
  
    gdk_gc_set_foreground (draw_gc, &lat_map[i].c);
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
  GtkWidget *widget = drawing_area;

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
    int x, y;
    ID_to_xy (n->ID, &x, &y);
    gdk_draw_arc (pixmap,
		  widget->style->black_gc,
		  TRUE,
		  x - 4,y - 4,
		  8,8,
		  (gint16)0, (gint16)64*360);
    
    if (drawids) {
      GdkFont *f= gdk_font_load ("-*-courier-*-r-*-*-*-*-*-*-*-*-*-*");
      char IDs[128];
      ID_to_string (n->ID, IDs);
      int fudge = -10;
      if (x < WINX/2) fudge = 0;
      gdk_draw_string (pixmap,
		       f,
		       widget->style->black_gc,
		       x + fudge,y,
		       IDs);
    }

    if (n->draw) {
      
      if (n->res && 
	  gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (check_immed_succ)))
	{
	  int a,b;
	  set_foreground_lat (n->res->resok->fingers[1].a_lat); 
	  ID_to_xy (n->res->resok->fingers[1].x, &a, &b);
	  draw_arrow (x,y,a,b, draw_gc);
	}

      if (n->res && 
	  gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (check_fingers))) {
	for (unsigned int i=1; i < n->res->resok->fingers.size (); i++) {
	  int a,b;
	  set_foreground_lat (n->res->resok->fingers[i].a_lat); 
	  ID_to_xy (n->res->resok->fingers[i].x, &a, &b);
	  draw_arrow (x,y,a,b, draw_gc);
	}
      }

      if (n->ressucc && 
	  gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (check_succ_list))) {
	for (unsigned int i=1; i < n->ressucc->resok->succ.size (); i++) {
	  draw_arc (n->ID, n->ressucc->resok->succ[i].x, drawing_area->style->black_gc);
	}
      }

      if (n->restoes && 
	  gtk_toggle_button_get_active (GTK_TOGGLE_BUTTON (check_neighbors))) {
	for (unsigned int i=0; i < n->restoes->resok->toes.size (); i++) {
	  int a,b;
	  ID_to_xy (n->restoes->resok->toes[i].x, &a, &b);
	  set_foreground_lat (n->restoes->resok->toes[i].a_lat); 
	  draw_arrow (x,y,a,b,draw_gc);
	}
      }
    }
    
    n = nodes.next (n);
  }
  redraw ();
}

void
ID_to_string (chordID ID, char *str) 
{
  bigint little = (ID >> 144);
  int z = little.getsi ();
  sprintf(str, "%x", z);
}

double 
ID_to_angle (chordID ID)
{
  bigint little = (ID >> 144);
  int z = little.getsi (); 
  return (z/65535.0) * 2 * 3.14159;
}

void
ID_to_xy (chordID ID, int *x, int *y)
{
 
  double angle = ID_to_angle (ID);
  double radius = (WINX - 20)/2;

  *x = (int)(WINX/2 + sin (angle)*radius);
  *y = (int)(WINY/2 - cos (angle)*radius);
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
  short port = 0;
  interval = -1;
  color_file = ".viscolors";

  int ch;
  while ((ch = getopt (argc, argv, "j:a:l:f:i")) != -1) {
    switch (ch) {
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
  default:
    usage ();
    }
  };

  if (host == "not set")
    usage ();

  initgraf ();

  get_fingers (host, port);
  get_succ (host, port);

  if (interval > 0) {
    warn << "enabling auto-update at " << interval << " second intervals\n";
    delaycb (interval, 0, wrap (&update));
  }
 
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
