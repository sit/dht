HWS = \
	proposal.tex

HWS_PS = $(HWS:.tex=.ps)

PLOTS = $(wildcard *.gnuplot)
EPSPLOTS = $(PLOTS:.gnuplot=.eps2)

FIGS = $(wildcard *.fig)
EPSFIGS = $(FIGS:.fig=.eps)

all : $(EPSFIGS) $(EPSPLOTS) $(HWS_PS)

%.eps: %.fig
	fig2dev -L eps $< $@

%.eps2: %.gnuplot
	gnuplot $<

%.ps: %.tex proposal.bib
	latex $*
	bibtex $*
	latex $*
	latex $*
	dvips $* -t letter -o $@

clean:
	rm -f *.dvi *.eps *.aux *.ps *.log *.blg *.bbl *~
