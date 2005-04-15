/*

g++ -c vis_viv.C -I /usr/local/include -I /usr/X11R6/include/gtk-2.0/ -I /usr/local/include/glib-2.0 -I /usr/X11R6/include/pango-1.0/ -I /usr/local/include/atk-1.0/

gcc -o vis vis_viv.o -L/usr/X11R6/lib -lgdk-x11-2.0 -lstdc++ -lgtk-x11-2.0

 * Copyright (c) 2003-2005 Frank to Frank (via Emil and Frans)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "gtk/gtk.h"
#include "math.h"
#include "getopt.h"
#include <stdio.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

using namespace std;

#define WINX 700
#define WINY 700
#define PI 3.14159
#define  uint int
#define ulong long

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
  { DRAW_IMMED_SUCC, "tails", NULL, GTK_SIGNAL_FUNC (draw_toggle_cb) },
};

unsigned int check_get_state (void);
void check_set_state (unsigned int newstate);

static int running = 0;
static GdkPixmap *pixmap = NULL;
static GtkWidget *window = NULL;
static GtkWidget *drawing_area = NULL;
static GtkWidget *drawing_area_r = NULL;
static GdkGC *draw_gc = NULL;
static GdkColormap *cmap = NULL;
static GdkFont *courier10 = NULL;
static GtkAdjustment *bar = NULL;
static GtkWidget *scroll;
static GdkColor red;
static GdkColor search_color;

static GtkWidget *last_clicked;
static GtkWidget *total_nodes;

static long interval = 1000000;
static char *color_file;
static bool drawids = false;

static ifstream in;

static GdkGCValues GCValues;

static float zoomx = 1.0;
static float zoomy = 1.0;
static float centerx = 0.0;
static float centery = 0.0;
static int radius = 6;

static bool displaysearch = false;

static long begin = 0;
static long endofsim = 1000;
static long curtime;

struct color_pair {
  GdkColor c;
  unsigned long lat;
};

vector<color_pair> lat_map;

struct f_node {
  int id;
  vector<float> coords;
  vector<float> coords_prev;
  vector<int> neighbors;

  unsigned int draw;
  bool selected;
  bool highlight;
  int error;

  f_node (int ip) : id (ip), selected(false), highlight(false) {
    draw = check_get_state ();
  };
  ~f_node () {};
};

vector<f_node> nodes;

void initgraf ();
void init_color_list (char *filename);
void draw_arrow (int fromx, int fromy, 
		 int tox, int toy, GdkGC *draw_gc);

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
void scroll_cb (GtkAdjustment *adj, gpointer data);

void quit_cb (GtkWidget *widget, gpointer data);
void redraw_cb (GtkWidget *widget, gpointer data);
void search_cb (GtkWidget *widget, gpointer data);
void zoom_in_cb (GtkWidget *widget, gpointer data);
void dump_cb (GtkWidget *widget, gpointer data);
void gtk_poll ();
void draw_ring ();

int
find (int n) {
  for (uint i = 0; nodes.size (); i++) {
    if (nodes[i].id == n) {
      return i;
    }
  }
  cerr << "couldn't find node with ID " << n << "\n";
  return 0;
}

void 
print ()
{
  for (uint i = 0; i < nodes.size (); i++) {
    printf ("node %u is %16d\n", i, nodes[i].id);
  }
}

void
add_node (int n) {
  for (uint i = 0; i < nodes.size (); i++) {
    if (nodes[i].id > n) {
      nodes.insert (nodes.begin() + i, f_node(n));
      return;
    }
  }
  nodes.push_back (f_node(n));
}

// --- drawing ----------------------------------------------------------------


void
node_to_xy (f_node *n, int *x, int *y, int prev = 0)
{
  if (prev && n->coords_prev.size () > 1) {
    *x = (int)(WINX/2 + ((n->coords_prev[0] -centerx)/zoomx)*WINX);
    *y = (int)(WINX/2 + ((n->coords_prev[1] -centery)/zoomy)*WINY);
  } else {
    *x = (int)(WINX/2 + ((n->coords[0] - centerx)/zoomx)*WINX);
    *y = (int)(WINX/2 + ((n->coords[1] - centery)/zoomy)*WINY);
  }
}



int
xy_to_ID (int sx, int sy)
{
  int min = RAND_MAX;
  int closest = nodes[0].id;
  for (uint i = 0; i < nodes.size (); i++) {
    int x,y;
    node_to_xy (&nodes[i], &x, &y);
    int dist = (sx - x)*(sx - x) + (sy - y)*(sy - y);
    if (dist < min) {
      closest = nodes[i].id;
      min = dist;
    }
  }
  return closest;
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
redraw() 
{
  GdkRectangle update_rect;
  
  update_rect.x = 0;
  update_rect.y = 0;
  update_rect.width = WINX;
  update_rect.height = WINY;

  gtk_widget_draw(drawing_area, &update_rect);
  gtk_widget_show(scroll);
}

void
draw_arrow (int fromx, int fromy, 
	    int tox, int toy, GdkGC *draw_gc)
{
  gdk_draw_line (pixmap,
		 draw_gc,
		 fromx,fromy,
		 tox,toy);

  float t = std::atan2 ((float) (tox - fromx), (float) (toy - fromy));
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

  gdk_draw_polygon (pixmap, draw_gc, true, head, 3);
}


void
draw_node (f_node *iter)
{
  int x, y, oldx, oldy;
  int rad = (radius + 2);

  node_to_xy (iter, &x, &y);
  node_to_xy (iter, &oldx, &oldy, 1);
  //rotation?

  set_foreground_lat (iter->error);

  //draw circle
  gdk_draw_arc (pixmap, draw_gc, TRUE, x - radius, y - radius,
		2*radius, 2*radius, (gint16)0, (gint16)64*360);

  gdk_draw_line (pixmap,
		 draw_gc,
		 oldx,oldy,
		 x,y);

  //draw circle on selected nodes
  if (iter->selected)
    gdk_draw_arc (pixmap, draw_gc, FALSE, x - rad, y - rad, 2*rad, 
		  2*rad, (gint16)0, (gint16)64*360);

  if (drawids) {
    char ids[128];
    sprintf (ids, "%d", iter->id);
    int fudge = -10;
    if (x < WINX/2) fudge = 0;
    gdk_draw_string (pixmap, courier10, drawing_area->style->black_gc, 
		     x + fudge, y, ids);
  }

}


void
draw_ring ()
{
  int x, y;
  GtkWidget *widget = drawing_area;

  //clear the draw
  gdk_draw_rectangle (pixmap, widget->style->white_gc, TRUE, 0, 0, WINX, WINY);
  gdk_draw_rectangle (pixmap, widget->style->black_gc, FALSE, 0, 0, 
		      WINX - 1, WINY - 1);

  for (vector<f_node>::iterator iter = nodes.begin (); iter != nodes.end (); 
       ++iter) {

    f_node fx = *iter;
    draw_node(&fx);

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

  zoomx = zoomx*1.3 + 0.1;
  zoomy = zoomy*1.3 + 0.1;

  if (zoomx > zoomy) zoomy = zoomx;
  if (zoomy > zoomx) zoomx = zoomy;

  cerr << "recenter (" << centerx << ", " << centery << ") " << " (" << zoomx << ", " << zoomy << ")\n";
}

void
update () 
{
}

// --- process events ----------------------------------------------------------

vector<string>
split(string line, string delims)
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

bool
doevent (ulong t)
{
  string line;
  ulong ts = curtime;
  bool step = false;

  if  (curtime >= endofsim) {
    return false;
  }

  while (getline (in, line)) {
    vector <string> words = split (line, " ");

    if (words.empty () || (words[0] != "vis")) 
      continue;

    ulong ts1 = atoi(words[1].c_str ());

    assert (ts1 >= curtime);
    curtime = ts1;

    //vis ts1 node ID [coords]
    if (words[2] == "node") {
      int id = strtol (words[3].c_str (), NULL, 16);
      add_node (id);
      f_node *n = &nodes[find(id)];
      for (int i = 4; i < words.size (); i++)
	n->coords.push_back(atoi(words[i].c_str()));
    }


    //vis ts1 step contacted error newpos
    if (words[2] == "step" && words.size () > 6) {
      int id = strtol (words[3].c_str (), NULL, 16);
      int contacted = strtol (words[4].c_str (), NULL, 16);
      int error = strtol (words[5].c_str (), NULL, 16);
      int pos = find (id);
      f_node *n = &nodes[pos];
      n->error  = error;
      n->coords_prev = n->coords;
      n->coords.clear ();
      for (int i = 6; i < words.size (); i++)
	n->coords.push_back(atoi(words[i].c_str()));
      
      // XXX draw line to contacted?
    }

    
    gtk_main_iteration_do (0);
    if ((curtime >= endofsim) || step) break;

  }



  return (step);
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

  gdk_gc_get_values(draw_gc, &GCValues);

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
    p.lat = lat; 
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
  GtkWidget *search = gtk_button_new_with_label ("Stop");
  GtkWidget *run = gtk_button_new_with_label ("Run");
  GtkWidget *step = gtk_button_new_with_label ("Step");
  GtkWidget *quit = gtk_button_new_with_label ("Quit");
  GtkWidget *sep = gtk_vseparator_new ();
  GtkWidget *dump_to_file = gtk_button_new_with_label ("Save...");
  bar = (GtkAdjustment *) gtk_adjustment_new (0, 0, 1000000, 10, 
					  1000, 1000);
  scroll = gtk_hscrollbar_new ((GtkAdjustment *)bar);
  gtk_range_set_update_policy (GTK_RANGE (scroll), GTK_UPDATE_CONTINUOUS);

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
  gtk_box_pack_end (GTK_BOX (vbox), scroll, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), search, FALSE, FALSE, 0);
  gtk_box_pack_end (GTK_BOX (vbox), dump_to_file, FALSE, FALSE, 0);
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
  gtk_signal_connect_object (GTK_OBJECT (search), "clicked",
			       GTK_SIGNAL_FUNC (search_cb),
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
  gtk_signal_connect (GTK_OBJECT (bar), "value_changed",
			       GTK_SIGNAL_FUNC (scroll_cb),
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
  gtk_widget_show (search);
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
  gtk_widget_show (scroll);

  gtk_widget_show (window);

  init_color_list (color_file);

  if (!gdk_color_parse ("red", &red) ||
      !gdk_colormap_alloc_color (cmap, &red, FALSE, TRUE))
    cerr << "Couldn't allocate search color red\n";

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
search_cb (GtkWidget *widget, gpointer data)
{
  cerr << "stop cb\n";
  running = 0;
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
zoom_in_cb (GtkWidget *widget, gpointer data)
{
  recenter ();
  draw_ring ();
  redraw ();
}

gint
key_release_event (GtkWidget *widget, GdkEventKey *event, gpointer data)
{
  // warnx << "key pressed " << event->keyval << "\n";
  switch (event->keyval) {
  case 'n':
    {
      doevent (curtime + interval);
      break;
    }
  case 'q':
  case 'Q':
    quit_cb (NULL, NULL);
    break;
  case 'z':
    zoomx *= 1.5;
    zoomy *= 1.5;
    draw_ring ();
    break;
  case 'Z':
    zoomx *= 0.75;
    zoomy *= 0.75;
    draw_ring ();
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
  int ID = xy_to_ID ((int)event->x,(int)event->y);
  uint i = find (ID);
  if (event->button == 2) // middle button
    nodes[i].highlight = !nodes[i].highlight;
  else {
    nodes[i].selected = !nodes[i].selected;
    if (nodes[i].selected)
      check_set_state (nodes[i].draw);
  }

  char hosts[1024];
  sprintf (hosts, "ID: %4d", nodes[i].id);

  gtk_label_set_text (GTK_LABEL (last_clicked), hosts);
  draw_ring ();
  return TRUE;
}

void
run_cb (GtkWidget *widget, gpointer data)
{
  cerr << "run: interval is " << interval << "\n";
  running = 1;
  while (running) {
    endofsim = curtime + interval;
    doevent (0);
    char buf[1024];
    sprintf (buf, "Time: %10d", endofsim/1000);
    gtk_label_set_text (GTK_LABEL (total_nodes), buf);
    gtk_main_iteration_do (0);
    draw_ring ();
  }

}

void
step_cb (GtkWidget *widget, gpointer data)
{
  (void) doevent (curtime + interval);
}

void
scroll_cb (GtkAdjustment *adj, gpointer data)
{
  interval = (long)(adj->value);
  cerr << "interval set to " << interval << "\n";
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
	argv += 2;
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

