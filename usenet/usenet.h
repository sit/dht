// -*-c++-*-
/* $Id: usenet.h,v 1.3 2004/08/26 21:19:24 sit Exp $ */

/*
 *
 * Copyright (C) 2004, 2003 Emil Sit (sit@mit.edu)
 * Copyright (C) 2003 James Robertson (jsr@mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#ifndef USENET
#define USENET

/* usenet.C */
struct dbfe;
struct dhashclient;
extern dbfe *group_db, *header_db;
extern dhashclient *dhash;

/* config.C */
struct options {
  options ();
  ~options ();

  unsigned int client_timeout;
  u_int16_t listen_port;
  unsigned int peer_timeout;
  unsigned int sync_interval;

private:
  str parse_peer (vec<str> &av);
  friend bool parseconfig (options *op, str cf);
};

extern options *opt;

#endif /* USENET */
