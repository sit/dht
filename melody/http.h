// -*-c++-*-
/* $Id: http.h,v 1.1 2002/05/01 20:35:09 jsr Exp $ */

/*
 *
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
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

#ifndef _HTTP_H_
#define _HTTP_H_ 1

#include "amisc.h"
#include "parseopt.h"

inline str
str2upper (str s)
{
  mstr m (s.len ());
  for (u_int i = 0; i < s.len (); i++)
    m[i] = toupper (s[i]);
  return m;
}

inline str
str2lower (str s)
{
  mstr m (s.len ());
  for (u_int i = 0; i < s.len (); i++)
    m[i] = tolower (s[i]);
  return m;
}

/* Remove the first n bytes of uio and return it as a string */
str suio_remstr (suio *uio, size_t n);

/* Generate a unique hash from a string.  (Useful for creating file
 * names from URL's, etc.) */
str hashstr (str);

/* Return time in HTTP format */
str httptime ();

/* Return HTTP error message */
str httperror (int status, str statmsg, str url, str description);

struct httpparse {
  /* Remove and return an HTTP header line (including continuation
   * lines).  Returns NULL if it doesn't have a complete header line.
   * Returns "" (the zero-length string, no CR LR) when it hits the
   * empty line denoting the end of HTTP headers. */
  static str gethdr (suio *uio);

  /* Return the name of a header, CONVERTED TO LOWER-CASE.  I.e. for
   * "Pragma: no-cache" returns "pragma".  Returns NULL for bad
   * header. */
  static str hdrname (str hdr);

  /* Returns the value of a header.  hdr must be a valid header. */
  static str hdrval (str hdr);

  /* Parses the time in an HTTP header field and returns it, or returns
   * (time_t) -1 if time is in an invalid format. */
  static time_t parsetime (const char *text);

  /* Returns true iff val contains pragma */
  static bool checkpragma (str val, str pragma);

  /* Parse an http URL */
  static bool parseurl (str *host, u_int16_t *port, str *path, str url);

  /* Parses an HTTP request line, returns false if line was
   * ill-formatted. */
  static bool parsereq (str *method, str *url, str line);

  /* Parses an HTTP reply, returns false if line was ill-formatted. */
  static bool parseresp (int *status, str *msg, str line);


  bool firstdone;
  strbuf headers;

  httpparse () : firstdone (false) {}
  virtual ~httpparse () {}
  virtual bool dofirst (str line) = 0;
  virtual bool doheader (str name, str val) = 0;

  /* Returns 1 when complete, 0 if it needs more data, and -1 on error */
  int parse (suio *uio);
};

struct httpreq : public httpparse {
  str method;
  str host;
  u_int16_t port;
  str path;
  str url;
  str urlhash;

  bool nocache;
  time_t if_modified_since;
  ssize_t content_length;
  str authorization;
  str referer_host;
  u_int16_t r_port;
  str r_path;

  void reset () {
    nocache = false;
    if_modified_since = -1;
    content_length = -1;
    authorization = NULL;
    referer_host = NULL;
    r_port = 80;
    r_path = NULL;
  }
  httpreq () { reset (); }
  bool dofirst (str line);
  bool doheader (str name, str val);
};

struct httpresp : public httpparse {
  int status;
  str statusmsg;

  bool nocache;
  time_t date;
  time_t expires;
  time_t last_modified;
  ssize_t content_length;

  void reset () {
    statusmsg = NULL;
    nocache = false;
    date = expires = last_modified = -1;
    content_length = -1;
  }
  httpresp () { reset (); }
  bool dofirst (str line);
  bool doheader (str name, str val);
};

#endif /* !_HTTP_H_ */
