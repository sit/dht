.	\" initialization goo
.de ..
..
.fp 5 CW LucidaSansCW83
.ie \n(yr>=100 .ds Ps "ps: exec
.el .ds Ps PS
.de XX
\\X'\\$1'
.br
.sp -1
..
.nr XT 4
.nr pn 1
.nr HM 0i
.nr LL 7i
.ll 7i
.pl +.5i
.nr PS 20
.ps 20
.nr VS 22
.vs 22
.nr HY 0
.	\" a new slide
.de Sl
.bp
.tl '''\s8\\n(pn\s0'
.nr pn \\n(pn+1
.ce
\s+2\f3\\$1\fP\s-2
.nr PS 20
.ps 20
.nr VS 22
.vs 22
.LP
..
.am LP
.sp 
..
.am IP
.sp 0.5
..
.am P1
.ps 16
.vs 18
.ta +4n +4n +4n +4n +4n
..
.de Sx
.bp
.sp |0i
.nr PS 20
.ps 20
.nr VS 22
.vs 22
..
.ds CH
.nr HM 1i
.de GO
.ds CH
.nr HM 1.5i
..
.na
.EQ
delim $#
.EN
.TL
Serving DNS using Chord
.br
(or Pastry, or ...)
.br
Good idea, bad idea?
.AU
.ft 1
Russ Cox
.br
Athicha Muthitacharoen
.br
Robert Morris
.sp
.I "rsc,athicha,rtm@pdos.lcs.mit.edu
.LP
.ce 2
IPTPS '02
March 8, 2002
.	\" more initialization goo
.ds CH
.nr HM 1.5i
.	\" The slides
...
...
...
...
.Sl "Motivation
.LP
Before DNS, there was a global
.CW hosts.txt .
.LP
DNS is an attempt to distribute
.CW hosts.txt
but:
.IP
Configuring DNS properly is tedious and time-consuming.
.br
Everyone has to be a DNS admin.
.IP
I can't have a domain without finding someone to serve my names.
.IP
Easy to have locally-correct-but-globally-wrong configurations.
.LP
Peer-to-peer lookup services solve some of these problems:
.IP
Organization, replication, and much configuration done by P2P layer.
.IP
I don't need to run my own 24/7 DNS server.
.IP
Lack of hierarchy avoids half-broken configurations.
...
...
...
.Sl "Motivation, II
.LP
``Distributing authority for a database does not distribute
corresponding amounts of expertise.  Maintainers fix things
until they work, rather than until they work well, and want to use,
not understand, the systems they are provided.
Systems designers should anticipate this, and try to
compensate by technical means.''
.LP
\h'1i'Mockapetris and Dunlap, 1988
...
...
...
.Sl "DNS
.LP
IP-based DNS authentication
.IP
Trust IP layer; believe what you hear from those you trust.
.IP
Root servers are known.
.IP
Servers delegate authority to other IP addresses.
.IP
Verification induces lookup algorithm.
.LP
DNSSEC-based DNS authentication
.IP
Trust public key crypto; believe what is signed by those you trust.
.IP
Root server keys are known.
.IP
Servers delegate authority to other keys.
.IP
Verification leaves lookup completely unspecified.
.LP
With DNSSEC, can explore other lookup methods.
.LP
Peer-to-peer!
...
...
...
.Sl "DNS with P2P Hash Table
.LP
Look up SHA1(name, query-type).
.LP
(Maybe walk key hierarchy, maybe store records
with all relevant keys.)
.LP
Perfect match for ``distributed
.CW hosts.txt .''
.LP
Prototype implemented using Chord.
...
...
...
.Sl "Evaluation: Latency
.LP
Uncached latency is enormous.
.IP
$O( roman "log" sub 2 n)# RPCs per lookup.
.IP
Compare to conventional DNS: typically 1 or 2 RPCs per lookup.
.LP
Cached latency a little better, not much.
...
...
...
.Sl "Evaluation: Functionality
.LP
We have all the functionality of a distributed 
.CW hosts.txt .
.LP
And nothing more.
.IP
No support for ``ANY'' queries.
.IP
No dynamically-generated records.
.IP
No DNS-server-side load balancing (randomization).
.IP
No DNS-server-side proximity routing (Akamai).
...
...
...
.Sl "Evaluation: Ease of Administration
.LP
O'Reilly BIND+DNS book lists 13 common problems.
.LP
9 are arguably software deficiencies (needing
to restart the server after config changes, ...).
.LP
4 are actually protocol-specific problems; we just stir them around.
.IP
Missing subdomain server delegation → missing public key delegation
.IP
Bad subdomain delegation → bad public key delegation
.IP
Network outage → different network outage
.LP
1 is gone:
.IP
Slave server can't load zone → caching is transparent
.LP
But why trust random servers rather than run my own?
.IP
Chicken and egg problem or fundamental worry?
.IP
Comparison with IP routing? [XXX]
...
...
...
.Sl "Evaluation: Robustness
.LP
Robustness inherited from P2P layer:
.IP
Better fault tolerance against DoS attacks.
.IP
Hard to target specific records \(em network routes around damage.
.IP
No central points of failure.
.LP
Then again:
.IP
DNS DoS attacks are so 1998.
.IP
The central DNS servers have very good reliability.
.IP
New DoS: inserting lots of bogus records.
.IP
How do we tell DoS from normal usage?
.br
Why can't I have a billion records for my personal domain?
...
...
...
.Sl "Conclusions
.LP
If it were 25 years ago, P2P DNS might be worth considering.
.LP
Current DNS is more than just distributed
.CW hosts.txt .
.LP
Some benefits, mostly drawbacks.
.LP
Addressing a non-issue: more benefit from fixing BIND's UI.
.LP
Better the devil you know than the devil you don't.
...
...
...
