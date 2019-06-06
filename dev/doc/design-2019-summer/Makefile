dot := $(wildcard *.dot)
svg := $(dot:.dot=.svg)

all: $(svg)

%.svg: %.dot
	dot -Tsvg < $< > $@
