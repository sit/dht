#include "route.h"
#include "chord_impl.h"
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>

//
// Finger table routing 
//

route_chord::route_chord (ptr<vnode> vi, chordID xi) : 
  route_iterator (vi, xi) {};

route_chord::route_chord (ptr<vnode> vi, chordID xi,
			  rpc_program uc_prog,
			  int uc_procno,
			  ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args)
{
}

void
route_chord::first_hop (cbhop_t cbi, ptr<chordID> guess)
{
  cb = cbi;

  chordID myID = v->my_ID ();
  
  vec<chordID> failed_null;
  ptr<location> l;
  if (guess) l = v->locations->lookup (*guess);
  // if the guess isn't cached, ignore it
  if (!l) l = v->closestpred (x, failed_null);
  search_path.push_back (l);

  next_hop (); // deliver the upcall
}

void
route_chord::next_hop ()
{
  ptr<location> n = search_path.back ();
  make_hop (n);
}


void
route_chord::send (ptr<chordID> guess)
{
  first_hop (wrap (this, &route_chord::send_hop_cb), guess);
}

void
route_chord::send_hop_cb (bool done)
{
  if (!done) next_hop ();
}

void
route_chord::make_hop (ptr<location> n)
{
  ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
  arg->x = x;
  arg->failed_nodes.setsize (failed_nodes.size ());
  for (unsigned int i = 0; i < failed_nodes.size (); i++)
    arg->failed_nodes[i] = failed_nodes[i];

  if (do_upcall) {
    int arglen;
    char *marshalled_args = route_iterator::marshall_upcall_args (&prog,
								  uc_procno,
								  uc_args,
								  &arglen);
								  
    arg->upcall_prog = prog.progno;
    arg->upcall_proc = uc_procno;
    arg->upcall_args.setsize (arglen);
    memcpy (arg->upcall_args.base (), marshalled_args, arglen);
    xfree (marshalled_args);
  } else
    arg->upcall_prog = 0;

  chord_testandfindres *nres = New chord_testandfindres (CHORD_OK);
  v->doRPC (n, chord_program_1, CHORDPROC_TESTRANGE_FINDCLOSESTPRED,
	    arg, nres,
	    wrap (this, &route_chord::make_hop_cb, deleted, nres));
}

ptr<location>
route_chord::pop_back () 
{
  return search_path.pop_back ();
}


void
route_chord::on_failure (ptr<location> f)
{
  failed_nodes.push_back (f->id ());
  v->alert (search_path.back (), f);
  warn << v->my_ID () << ": " << f->id () << " is down.  Now trying "
       << search_path.back ()->id () << "\n";
  next_hop ();
}

void
route_chord::make_hop_cb (ptr<bool> del,
			  chord_testandfindres *res, clnt_stat err)
{
  if (*del) {
    delete res;
    return;
  }
  if (err) {
    //back up
    ptr<location> last_node_tried = pop_back ();
    if (search_path.size () == 0)
      search_path.push_back (v->my_location ());

    on_failure (last_node_tried);
  } 

  else if (res->status == CHORD_STOP) {
    r = CHORD_OK;
    cb (true);
  } 

  else if (res->status == CHORD_INRANGE) { 
    // found the successor
    ptr<location> n0 =
      v->locations->insert (make_chord_node (res->inrange->n[0]));
    if (!n0) {
      warnx << v->my_ID () << ": make_hop_cb: inrange node ("
	    << res->inrange->n[0] << ") not valid vnode!\n";
      assert (0);
    }
    search_path.push_back (n0);
    successors_.clear ();
    for (size_t i = 0; i < res->inrange->n.size (); i++)
      successors_.push_back (make_chord_node (res->inrange->n[i]));
    
    cb (true);
  } 

  else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    ptr<location> last = search_path.back ();
    chord_node n = make_chord_node (res->notinrange->n);
    if (last->id () == n.x) {   
      warnx << v->my_ID() << ": make_hop_cb: node " << last->id ()
	   << "returned itself as best pred, looking for "
	   << x << "\n";
      r = CHORD_ERRNOENT;
      cb (true);
    } else {
      // make sure that the new node sends us in the right direction,
      chordID olddist = distance (search_path.back ()->id (), x);
      chordID newdist = distance (n.x, x);
      if (newdist > olddist) {
	warnx << "XXXXXXXXXXXXXXXXXXX WRONG WAY XXXXXXXXXXXXX\n";
	warnx << v->my_ID() << ": make_hop_cb: went in the wrong direction:"
	      << " looking for " << x << "\n";
	// xxx surely we can do something more intelligent here.
	print ();
	warnx << v->my_ID() << ": " << search_path.back ()->id ()
	      << " sent me to " << n.x
	      << " looking for " << x << "\n";
	warnx << "XXXXXXXXXXXXXXXXXXX WRONG WAY XXXXXXXXXXXXX\n";
      }
      
      ptr<location> n0 = v->locations->insert (n);
      if (!n0) {
	warnx << v->my_ID () << ": make_hop_cb: notinrange node ("
	      << res->notinrange->n << ") not valid vnode!\n";
	assert (0);
      }
      search_path.push_back (n0);
      
      successors_.clear ();
      for (size_t i = 0; i < res->notinrange->succs.size (); i++)
	successors_.push_back (make_chord_node (res->notinrange->succs[i]));

      assert (search_path.size () <= 1000);
      cb (false);
    }
  } else {
    fatal << "status was unreasonable: " << res->status << "\n";
  }
  delete res;
}

