
BASE_URL = https://datasets.imdbws.com

OUT = \
 name.basics.tsv.gz \
 title.basics.tsv.gz \

all: $(OUT)

%.tsv.gz:
	curl --progress-bar --remote-name  $(BASE_URL)/$@

clean:
	rm -f $(OUT)
