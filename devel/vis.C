#include "chord.h"
#include "gtk/gtk.h"
#include "gdk/gdk.h"
#include "math.h"

#define WINX 600
#define WINY 600
#define PI 3.14159

/* GTK stuff */
static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static GdkColor red, green, blue;

struct f_node {
  chordID ID;
  chord_getfingers_ext_res *res;
  ihash_entry <f_node> link;
  bool draw;

  f_node (chordID i, chord_getfingers_ext_res *r) : 
    ID (i), res (r), draw (true) {};
  ~f_node () { delete res; };
};

void setup ();
void get_fingers (str host, short port);
void get_fingers (chordID ID, str host, short port);
void get_fingers_cb (chord_getfingers_ext_res *res, chordID ID, clnt_stat err);
void add_fingers (chordID ID, chord_getfingers_ext_res *res);
void initgraf (int argc, char **argv);
static gint configure_event (GtkWidget *widget, GdkEventConfigure *event);
static gint expose_event (GtkWidget *widget, GdkEventExpose *event);
static gint delete_event(GtkWidget *widget, GdkEvent *event, gpointer data);
static gint button_down_event (GtkWidget *widget,
			       GdkEventButton *event,
			       gpointer data);
void draw_all_cb (GtkWidget *widget, gpointer data);
void draw_none_cb (GtkWidget *widget, gpointer data);
void quit_cb (GtkWidget *widget, gpointer data);
void redraw();
void draw_ring ();
void ID_to_xy (chordID ID, int *x, int *y);
chordID xy_to_ID (int sx, int sy);
void ID_to_string (chordID ID, char *str);
double ID_to_angle (chordID ID);
int main (int argc, char** argv);

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
get_fingers (str host, short port) 
{
  chordID ID = make_chordID (host, port);
  get_fingers (ID, host, port);
}

void
get_fingers (chordID ID, str host, short port) 
{
  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, chord_program_1, 
			       (sockaddr *)&(saddr));

  if (c == NULL) 
    fatal << "locationtable::doRPC: couldn't aclnt::alloc\n";

  chord_vnode n;
  n.n = ID;
  chord_getfingers_ext_res *res = New chord_getfingers_ext_res ();
  clnt_stat err = c->scall (CHORDPROC_GETFINGERS_EXT, &n, res);

  if (err) 
    fatal << "getfingers failed: " << err << "\n";
  if (res->status)
    fatal << "getfingers failed: " << res->status << "\n";

  add_fingers (ID, res);
}

void
add_fingers (chordID ID, chord_getfingers_ext_res *res) 
{
  if (nodes[ID]) return;

  warn << "added " << ID << "\n";
  f_node *n = New f_node (ID, res);
  nodes.insert (n);
  for (unsigned int i=0; i < res->resok->fingers.size (); i++) {
    if ( nodes[res->resok->fingers[i].x] == NULL) 
      get_fingers (res->resok->fingers[i].x, res->resok->fingers[i].r.hostname,
		   res->resok->fingers[i].r.port);
  }
  draw_ring ();
}


void
initgraf (int argc, char **argv)
{
  gtk_init (&argc, &argv);

  window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
  drawing_area = gtk_drawing_area_new();
  gtk_drawing_area_size ((GtkDrawingArea *)drawing_area, WINX, WINY);

  GtkWidget *draw_all = gtk_button_new_with_label ("Show All");
  GtkWidget *draw_none = gtk_button_new_with_label ("Show None");
  GtkWidget *quit = gtk_button_new_with_label ("Quit");
  GtkWidget *sep = gtk_vseparator_new ();

  //organize things into boxes
  GtkWidget *vbox = gtk_vbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), draw_all, TRUE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), draw_none, TRUE, FALSE, 0);
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
  gtk_signal_connect_object (GTK_OBJECT (quit), "clicked",
			       GTK_SIGNAL_FUNC (quit_cb),
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
  gtk_widget_show (draw_none);
  gtk_widget_show (quit);
  gtk_widget_show (sep);
  gtk_widget_show (hbox);
  gtk_widget_show (vbox);
  gtk_widget_show (window);

  draw_gc = gdk_gc_new (drawing_area->window);
  assert (draw_gc);
  cmap = gdk_colormap_get_system ();
  if (!gdk_color_parse ("green", &green) ||
      !gdk_colormap_alloc_color (cmap, &green, FALSE, TRUE))
    fatal << "couldn't get the color I wanted\n";
  if (!gdk_color_parse ("blue", &blue) ||
      !gdk_colormap_alloc_color (cmap, &blue, FALSE, TRUE))
    fatal << "couldn't get the color I wanted\n";
  if (!gdk_color_parse ("red", &red) ||
      !gdk_colormap_alloc_color (cmap, &red, FALSE, TRUE))
    fatal << "couldn't get the color I wanted\n";
}

void 
quit_cb (GtkWidget *widget,
	 gpointer data) {  
  gtk_main_quit ();
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




gint 
delete_event(GtkWidget *widget,
             GdkEvent  *event,
	     gpointer   data )
{
  gtk_main_quit ();
  return(FALSE);
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
		  x,y,
		  8,8,
		  (gint16)0, (gint16)64*360);
    
#ifdef DRAWIDS
    GdkFont *f= gdk_font_load ("-*-courier-*-r-*-*-*-*-*-*-*-*-*-*");
    char IDs[128];
    ID_to_string (n->ID, IDs);
    gdk_draw_string (pixmap,
		     f,
		     widget->style->black_gc,
		     x,y,
		     IDs);
#endif
    if (n->draw)
      for (unsigned int i=0; i < n->res->resok->fingers.size (); i++) {
	int a,b;
	if (n->res->resok->fingers[i].a_lat < 2000000) // < 20 ms
	  gdk_gc_set_foreground (draw_gc, &red);
	else if (n->res->resok->fingers[i].a_lat < 10000000) // [20, 100] ms
	  gdk_gc_set_foreground (draw_gc, &green);
	else // > 100 ms
	  gdk_gc_set_foreground (draw_gc, &blue);

	ID_to_xy (n->res->resok->fingers[i].x, &a, &b);
	gdk_draw_line (pixmap,
		       draw_gc,
		       x,y,
		       a,b);
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
  double radius = (WINX)/2 - 10;

  *x = 5 + (int)((WINX - 5)/2 + sin (angle)*radius);
  *y = 5 + (int)((WINY - 5)/2 - cos (angle)*radius);
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

int
main (int argc, char** argv) 
{

  setprogname (argv[0]);
  sfsconst_init ();
  random_init ();

  setup ();
  initgraf (argc, argv);

  if (argc != 3) 
    fatal << "vis [gtk options] <IP in dotted decimal> <port>\n";

  get_fingers (argv[1], atoi (argv[2]));

  gtk_main ();
  gtk_exit (0);
}

