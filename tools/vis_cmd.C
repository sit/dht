#include <gtk/gtk.h>
#include <gdk/gdk.h>

#include <async.h>
#include <arpc.h>
#include <aios.h>
#include <rxx.h>

#include "vis.h"

vec<ptr<annotation> > annotations;

static void do_accept (int fd);
static void process_line (ptr<aios> aio, const str data, int err);
static void dispatch (ptr<aios> aio, str cmd, vec<str> args);

void
setup_cmd (int p)
{
  int cmdfd = inetsocket (SOCK_STREAM, p);
  if (cmdfd < 0)
    fatal << "creating command socket: %m\n";
  if (listen (cmdfd, 5) < 0)
    fatal << "listening on command socket: %m\n";
  fdcb (cmdfd, selread, wrap (do_accept, cmdfd));
  warnx << "Listening on " << p << "\n";
}

void
do_accept (int lfd)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0)
    fatal << "accept on command socket: %m\n";

  ptr<aios> aio = aios::alloc (fd);
  aio->settimeout (600);
  aio->setdebug (strbuf ("%d", fd));
  aio->readline (wrap (process_line, aio));
}

void
process_line (ptr<aios> aio, const str data, int err)
{
  if (err < 0) {
    if (err != ETIMEDOUT)
      fatal << aio->fdno () << ": unexpected error " << err << "\n";
    warn << aio->fdno () << ": timed out.\n";
    return;
  }
  if (!data || !data.len ()) {
    warn << aio->fdno () << ": data oops.\n";
    return;
  }

  vec<str> cmdargs;
  int n = split (&cmdargs, rxx ("\\s+"), data);
  assert (n >= 0);
  if (n == 0)
    return;
  str cmd = cmdargs.pop_front ();
  dispatch (aio, cmd, cmdargs);
}

void
dispatch (ptr<aios> aio, str cmd, vec<str> args)
{
  bool done (false);
  if (cmd == "highlight") {
    chordID id;
    if (args.size () && str2chordID (args[0], id)) {
      f_node *n = nodes[id];
      if (n) {
	bool on (true);
	if (args.size () > 1)
	  on = atoi (args[1]) == 1;
	n->highlight = on;
      }
    }
  }
  else if (cmd == "select") {
    chordID id;
    if (args.size () && str2chordID (args[0], id)) {
      f_node *n = nodes[id];
      if (n) {
	bool on (true);
	if (args.size () > 1)
	  on = atoi (args[1]) == 1;
	n->selected = on;
      }
    }
  }
  else if (cmd == "arc") {
    chordID id_a, id_b;
    if (args.size () > 1 &&
	str2chordID (args[0], id_a) && str2chordID (args[1], id_b))
    {
      ptr<annotation> a = New refcounted<arc_annotation> (id_a, id_b);
      annotations.push_back (a);
    }
  }
  else if (cmd == "arrow") {
    chordID id_a, id_b;
    if (args.size () > 1 &&
	str2chordID (args[0], id_a) && str2chordID (args[1], id_b))
    {
      int xa, ya, xb, yb;
      ID_to_xy (id_a, &xa, &ya);
      ID_to_xy (id_b, &xb, &yb);
      ptr<annotation> a = New refcounted<arrow_annotation> (xa, ya, xb, yb);
      annotations.push_back (a);
    }
  }
  else if (cmd == "list") {
    f_node *n = nodes.first ();
    while (n) {
      aio << n->ID << " "
	  << n->host << " "
	  << n->port << " "
	  << n->vnode_num;
      for (size_t i = 0; i < n->coords.size (); i++) {
	aio << " " << (int) n->coords[i];
      }
      aio << "\r\n";
      n = nodes.next (n);
    }
  }
  else if (cmd == "reset") {
    draw_nothing_cb (NULL, NULL);
  }
  else if (cmd == "quit") {
    done = true;
  }
  aio << ".\r\n";
  
  /* Re-register for reading additional commands */
  if (!done)
    aio->readline (wrap (process_line, aio));
}


// Pure virtual destructors still need definitions
annotation::~annotation () {}

void
arrow_annotation::draw (bool ggeo, GtkWidget *drawing_area)
{
  draw_arrow (xa, ya, xb, yb, drawing_area->style->black_gc);
}

void
arc_annotation::draw (bool ggeo, GtkWidget *drawing_area)
{
  draw_arc (id_a, id_b, drawing_area->style->black_gc);
}
