REPORT=paper
LATEX=latex
BIBTEX=bibtex
REF=paper
MPOST=mpost

SRCS=$(REPORT).tex

REFS=$(REF).bib

all: $(REPORT).ps

# log.ps: log.mp
# bash -c "export TEX=latex; $(MPOST) log.mp"

$(REPORT).dvi: $(SRCS) $(REFS)
	$(LATEX) $(REPORT)
	# $(BIBTEX) $(REPORT)
	$(LATEX) $(REPORT)
	$(LATEX) $(REPORT)

$(REPORT).pdf: $(SRCS) $(REFS)
	ps2pdf $(REPORT).ps

$(REPORT).ps: $(REPORT).dvi
	dvips $(REPORT).dvi -o $(REPORT).ps
	# dvips -T8.5,11 $(REPORT).dvi -o $(REPORT).ps

tidy:
	rm -f *.dvi *.aux *.log *.blg *.bbl

clean:
	rm -f *.dvi *.aux *.log *.blg *.bbl $(REPORT).ps

