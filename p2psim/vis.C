#include "chord.h"
#include "gtk/gtk.h"
#include "gdk/gdk.h"
#include "math.h"
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
using namespace std;

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
  { DRAW_DEBRUIJN,  "debruijn",  NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_IMMED_PRED, "immed. pred", NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_FINGERS,    "fingers",     NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
  { DRAW_TOES,       "neighbors",   NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) }
};

unsigned int check_get_state (void);
void check_set_state (unsigned int newstate);

static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GtkWidget *drawing_area_r = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static GdkFont *courier10 = NULL;

static GtkWidget *last_clicked;
static GtkWidget *total_nodes;

static short interval = 1;
static char *color_file;
static bool drawids = false;

static ifstream in;

static GdkColor highlight_color;
static char *highlight = "cyan4"; // consistent with old presentations
static GdkColor search_color;

static float zoomx = 1.0;
static float zoomy = 1.0;
static float centerx = 0.0;
static float centery = 0.0;

static bool ggeo = false;

struct color_pair {
  GdkColor c;
  unsigned long lat;
};

vector<color_pair> lat_map;

struct f_node {
  ConsistentHash::CHID id;
  vector<float> coords;
  ConsistentHash::CHID pred;
  ConsistentHash::CHID succ;
  ConsistentHash::CHID debruijn;
  vector<ConsistentHash::CHID> succlist;
  vector<ConsistentHash::CHID> dfingers;
  unsigned int draw;
  bool selected;
  bool highlight;
  
  f_node (ConsistentHash::CHID n) : id (n), selected(false), highlight(false) {
    draw = check_get_state ();
    pred = n;
    succ = n;
  };
  ~f_node () {};
};

vector<f_node> nodes;

void initgraf ();
void init_color_list (char *filename);
void draw_arrow (int fromx, int fromy, 
		 int tox, int toy, GdkGC *draw_gc);
void draw_arc (ConsistentHash::CHID from, ConsistentHash::CHID to, GdkGC *draw_gc);

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

void run_cb (GtkWidget *widget, gpointer data);
void step_cb (GtkWidget *widget, gpointer data);

void quit_cb (GtkWidget *widget, gpointer data);
void redraw_cb (GtkWidget *widget, gpointer data);
void update_cb (GtkWidget *widget, gpointer data);
void zoom_in_cb (GtkWidget *widget, gpointer data);
void geo_cb (GtkWidget *widget, gpointer data);
void dump_cb (GtkWidget *widget, gpointer data);
void gtk_poll ();
void draw_ring ();

uint
find (ConsistentHash::CHID n) {
  for (uint i = 0; nodes.size (); i++) {
    if (nodes[i].id == n) {
      return i;
    }
  }
  assert (0);
  return 0;
}

void 
print ()
{
  for (uint i = 0; i < nodes.size (); i++) {
    printf ("node %u is %16qx\n", i, nodes[i].id);
  }
}

void
add_node (ConsistentHash::CHID n) {
  for (uint i = 0; i < nodes.size (); i++) {
    if (nodes[i].id > n) {
      nodes.insert (nodes.begin() + i, f_node(n));
      return;
    }
  }
  nodes.push_back (f_node(n));
}


// --- drawing ----------------------------------------------------------------
double 
ID_to_angle (ConsistentHash::CHID ID)
{
  int z = ID >> (NBCHID - 16);
  return (z/65535.0) * 2 * 3.14159;
}

void
ID_to_xy (ConsistentHash::CHID ID, int *x, int *y)
{
  if (ggeo) {
    assert (0);
  } else {
    double angle = ID_to_angle (ID);
    double radius = (WINX - 60)/2;
    *x = (int)(WINX/2 + sin (angle)*radius);
    *y = (int)(WINY/2 - cos (angle)*radius);
  }
}

ConsistentHash::CHID
xy_to_ID (int sx, int sy)
{
  int min = RAND_MAX;
  ConsistentHash::CHID closest = nodes[0].id;
  for (uint i = 0; i < nodes.size (); i++) {
    int x,y;
    ID_to_xy (nodes[i].id, &x, &y);
    int dist = (sx - x)*(sx - x) + (sy - y)*(sy - y);
    if (dist < min) {
      closest = nodes[i].id;
      min = dist;
    }
  }
  return closest;
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

  for (vector<f_node>::iterator iter = nodes.begin (); iter != nodes.end (); 
       ++iter) {
    int radius = 5;
    ID_to_xy (iter->id, &x, &y);

    GdkGC *thisgc = widget->style->black_gc;
    if (iter->highlight) {
      radius = 7;
      gdk_gc_set_foreground (draw_gc, &highlight_color);
      thisgc = draw_gc;
    }

    gdk_draw_arc (pixmap,
		  thisgc,
		  TRUE,
		  x - radius, y - radius,
		  2*radius, 2*radius,
		  (gint16)0, (gint16)64*360);
    if (iter->selected) {
      radius += 2;
      gdk_draw_arc (pixmap,
		    thisgc,
		    FALSE,
		    x - radius, y - radius,
		    2*radius, 2*radius,
		    (gint16)0, (gint16)64*360);

      if (drawids) {
	char ids[128];
	sprintf (ids, "%16qx", iter->id);
	int fudge = -10;
	if (x < WINX/2) fudge = 0;
	gdk_draw_string (pixmap,
			 courier10,
			 widget->style->black_gc,
			 x + fudge,y,
			 ids);

      }

      if ((iter->draw & DRAW_IMMED_PRED) == DRAW_IMMED_PRED) {
	int a,b;
	ID_to_xy (iter->pred, &a, &b);
	draw_arrow (x,y, a, b, draw_gc);
      }

      if ((iter->draw & DRAW_IMMED_SUCC) == DRAW_IMMED_SUCC) {
	int a,b;
	ID_to_xy (iter->succ, &a, &b);
	draw_arrow (x,y, a, b, draw_gc);
      }

      if ((iter->draw & DRAW_SUCC_LIST) == DRAW_SUCC_LIST) {
	for (uint j = 0; j < iter->succlist.size (); j++) {
	  int a,b;
	  ID_to_xy (iter->succlist[j], &a, &b);
	  draw_arrow (x, y, a, b, draw_gc);
	}
      }

      if ((iter->draw & DRAW_DEBRUIJN) == DRAW_DEBRUIJN) {
	int a, b;
	ID_to_xy (iter->debruijn, &a, &b);
	// gdk_gc_set_foreground (draw_gc, &red);
	draw_arrow (x, y, a, b,  draw_gc);
	//	gdk_gc_set_foreground (draw_gc, &black);
	for (uint j = 0; j < iter->dfingers.size (); j++) {
	  int a, b;
	  ID_to_xy (iter->dfingers[j], &a, &b);
	  draw_arrow (x, y, a, b, draw_gc);
	}
      }

    }
  }
  
  redraw ();
}

void
recenter ()
{
  float minx=RAND_MAX, miny=RAND_MAX;
  float maxx=-RAND_MAX, maxy=-RAND_MAX;

  for (uint i = 0; i < nodes.size (); i++) {
    if (nodes[i].coords.size () > 0) {
      float x = nodes[i].coords[0];
      float y = nodes[i].coords[1];
      minx = (x < minx) ? x : minx;
      miny = (y < miny) ? y : miny;
      maxx = (x > maxx) ? x : maxx;
      maxy = (y > maxy) ? y : maxy;
    }
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
update () 
{
}

// --- process events ----------------------------------------------------------

vector<string>
split(string line, string delims = " \t")
{
  string::size_type bi, ei;
  vector<string> words;

  bi = line.find_first_not_of(delims);
  while(bi != string::npos) {
    ei = line.find_first_of(delims, bi);
    if(ei == string::npos)
      ei = line.length();
    words.push_back(line.substr(bi, ei-bi));
    bi = line.find_first_not_of(delims, ei);
  }

  return words;
}

void
doevent (bool single)
{
  string line;
  int a = interval;

  while (getline (in, line)) {
    vector <string> words = split (line);

    if (words.empty () || (words[0] != "vis")) 
      continue;

    if (words[2] == "node") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      add_node (id);
    }

    if (words[2] == "join") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      uint i = find (id);
      nodes[i].selected = true;
    }

    if (words[2] == "succ") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      ConsistentHash::CHID s = strtoull (words[4].c_str (), NULL, 16);
      uint i = find (id);
      nodes[i].succ = s;
    }

    if (words[2] == "pred") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      ConsistentHash::CHID p = strtoull (words[4].c_str (), NULL, 16);
      uint i = find (id);
      nodes[i].pred = p;
    }

    if (words[2] == "succlist") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      uint i = find (id);
      nodes[i].succlist.clear ();
      for (uint j = 4; j < words.size (); j++) {
	ConsistentHash::CHID s = strtoull (words[j].c_str (), NULL, 16);
	nodes[i].succlist.push_back(s);
      }
    }

    if (words[2] == "dfingers") {
      ConsistentHash::CHID id = strtoull (words[3].c_str (), NULL, 16);
      ConsistentHash::CHID d = strtoull (words[4].c_str (), NULL, 16);
      uint i = find (id);
      nodes[i].debruijn = d;
      nodes[i].dfingers.clear ();
      for (uint j = 5; j < words.size (); j++) {
	ConsistentHash::CHID s = strtoull (words[j].c_str (), NULL, 16);
	nodes[i].dfingers.push_back(s);
      }
    }

    a--;
    if (a <= 0) {
      draw_ring ();
      a = interval;
      if (single) return;
    }
  }

  draw_ring ();

  printf ("no events\n");
}
    

// --- UI ----------------------------------------------------------------

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
    cerr << "couldn't open " << filename << " using default color map\n";
    if (!gdk_color_parse ("red", &c) ||
	!gdk_colormap_alloc_color (cmap, &c, FALSE, TRUE))
      cerr << "couldn't get the color I wanted\n";
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
      cerr << "couldn't get the color I wanted\n";
    p.c = c;
    p.lat = lat * 1000; //convert from ms to microsec
    lat_map.push_back (p);
  }
  assert (lat_map.size () != 0);
}

void
initgraf ()
{
  courier10 = gdk_font_load ("-*-courier-*-r-*-*-12-*-*-*-*-*-*-*");

  window = gtk_window_new(GTK_WINDOW_TOPLEVEL);

  drawing_area = drawing_area_r = gtk_drawing_area_new();
  gtk_drawing_area_size ((GtkDrawingArea *)drawing_area_r, WINX, WINY);

  GtkWidget *select_all = gtk_button_new_with_label ("Select All");
  GtkWidget *select_none = gtk_button_new_with_label ("Select None");
  GtkWidget *hsep1 = gtk_hseparator_new ();
  GtkWidget *draw_nothing = gtk_button_new_with_label ("Reset");
  GtkWidget *hsep2 = gtk_hseparator_new ();
  last_clicked = gtk_label_new ("                    ");
  total_nodes = gtk_label_new ("                    ");
  for (size_t i = 0; i < NELEM (handlers); i++)
    handlers[i].widget = gtk_check_button_new_with_label (handlers[i].name);
  GtkWidget *hsep3 = gtk_hseparator_new ();
  GtkWidget *hsep4 = gtk_hseparator_new ();

  GtkWidget *in = gtk_button_new_with_label ("Recenter");
  GtkWidget *refresh = gtk_button_new_with_label ("Refresh All");
  GtkWidget *run = gtk_button_new_with_label ("Continue");
  GtkWidget *step = gtk_button_new_with_label ("Step");
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
  gtk_box_pack_start (GTK_BOX (vbox), last_clicked, FALSE, TRUE, 0);
  for (size_t i = 0; i < NELEM (handlers); i++)
    gtk_box_pack_start (GTK_BOX (vbox), handlers[i].widget, FALSE, FALSE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep3, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), hsep4, FALSE, TRUE, 0);
  gtk_box_pack_start (GTK_BOX (vbox), total_nodes, FALSE, TRUE, 0);

  gtk_box_pack_end (GTK_BOX (vbox), quit, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), run, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), step, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), refresh, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), dump_to_file, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), geo, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), in, FALSE, FALSE, 0);

  GtkWidget *hbox = gtk_hbox_new (FALSE, 0);
  gtk_box_pack_start (GTK_BOX (hbox), drawing_area_r, TRUE, FALSE, 0);
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
  gtk_signal_connect_object (GTK_OBJECT (run), "clicked",
			       GTK_SIGNAL_FUNC (run_cb),
			       NULL);
  gtk_signal_connect_object (GTK_OBJECT (step), "clicked",
			       GTK_SIGNAL_FUNC (step_cb),
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

  gtk_widget_show (select_all);
  gtk_widget_show (select_none);
  gtk_widget_show (draw_nothing);
  for (size_t i = 0; i < NELEM (handlers); i++)
    gtk_widget_show (handlers[i].widget);
  gtk_widget_show (last_clicked);
  gtk_widget_show (total_nodes);
  gtk_widget_show (in);
  gtk_widget_show (dump_to_file);
  gtk_widget_show (geo);
  gtk_widget_show (refresh);
  gtk_widget_show (quit);
  gtk_widget_show (run);
  gtk_widget_show (step);
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
    cerr << "Couldn't allocate highlight color " << highlight << "\n";
  if (!gdk_color_parse ("green", &search_color) ||
      !gdk_colormap_alloc_color (cmap, &search_color, FALSE, TRUE))
    cerr << "Couldn't allocate search color maroon\n";
}


// --- UI events ---------------------------------------------------------------

void
draw_toggle_cb (GtkWidget *widget, gpointer data)
{

  // Set the state of all the selected nodes to match what the button says.
  bool active = false;
  unsigned int flag = 0;
  // xxx Shouldn't we be comparing "widget" to "check_immed_succ"?
  //     Empircally, no, this is what works. Weird.
  for (size_t i = 0; i < NELEM (handlers); i++) {
    if (data == handlers[i].widget) {
      active = gtk_toggle_button_get_active
	(GTK_TOGGLE_BUTTON (handlers[i].widget));
      flag = handlers[i].flag;
      break;
    }
  }

  for (uint i = 0; i < nodes.size (); i++) {
    if (nodes[i].selected) {
      if (active)
	nodes[i].draw |= flag; 
      else
	nodes[i].draw &= ~flag;
    }
  }
  draw_ring ();
}

void 
select_all_cb (GtkWidget *widget, gpointer data) 
{
  for (uint i = 0; i < nodes.size (); i++) {
    nodes[i].selected = true;
  }
  draw_ring ();
}

void 
select_none_cb (GtkWidget *widget,
	        gpointer data) 
{
  for (uint i = 0; i < nodes.size (); i++) {
    nodes[i].selected = false;
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
  for (uint i = 0; i < nodes.size (); i++) {
    nodes[i].selected = false;
    nodes[i].highlight = false;
    nodes[i].draw = 0;
  }
  check_set_state (0);
  draw_ring ();
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

gint
key_release_event (GtkWidget *widget, GdkEventKey *event, gpointer data)
{
  // warnx << "key pressed " << event->keyval << "\n";
  switch (event->keyval) {
  case 'n':
    {
      doevent (true);
      break;
    }
  case 'q':
  case 'Q':
    quit_cb (NULL, NULL);
    break;
  default:
    break;
  }

  return (TRUE);
}


gint 
delete_event(GtkWidget *widget, GdkEvent  *event, gpointer   data )
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

static gint 
button_down_event (GtkWidget *widget, GdkEventButton *event, gpointer data) 
{
  ConsistentHash::CHID ID = xy_to_ID ((int)event->x,(int)event->y);
  uint i = find (ID);
  if (event->button == 2) // middle button
    nodes[i].highlight = !nodes[i].highlight;
  else {
    nodes[i].selected = !nodes[i].selected;
    if (nodes[i].selected)
      check_set_state (nodes[i].draw);
  }

  char hosts[1024];
  sprintf (hosts, "%64qx", nodes[i].id);

  gtk_label_set_text (GTK_LABEL (last_clicked), hosts);
  draw_ring ();
  return TRUE;
}


void
run_cb (GtkWidget *widget, gpointer data)
{
  doevent (false);
}


void
step_cb (GtkWidget *widget, gpointer data)
{
  doevent (true);
}

void 
quit_cb (GtkWidget *widget, gpointer data)
{
  gtk_exit (0);
}

void
usage ()
{
   cerr << "Usage: vis [-i] [-a <step>] <sim-file>" << endl;
}

int
main (int argc, char** argv) 
{
  string sim_file;
  color_file = ".viscolors";

  int ch;
  while ((ch = getopt (argc, argv, "a:i")) != -1) {
    switch (ch) {
    case 'a':
      {
	interval = atoi (optarg);
	argc -= 2;
	argv += 2;;
      }
      break;
    case 'i':
      {
	drawids = true;
	argc--;
	argv++;
      }
      break;
    default:
      usage ();
    }
  };

  gtk_init (&argc, &argv);

  if (argc < 2) {
    usage ();
    exit (-1);
  } else {
    sim_file = argv[1];
  }

  in.open(argv[1]);
  if(!in) {
    cerr << "Error: no file " << argv[1] << endl;
    exit (-1);
  }

  initgraf ();
  gtk_main ();
}
