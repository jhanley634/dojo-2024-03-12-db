
OUT = \
 out/ReadMe.html \
 out/01_bio.html \
 out/02_setup.html \

all: $(OUT)

out/%.html: ../%.md
	pandoc -s -o $@ $< --metadata title="intro to sqlalchemy"

out/%.html: %.md
	pandoc -s -o $@ $<

out/%.pdf: %.md
	pandoc -s -o $@ $<

clean:
	rm -f $(OUT)
