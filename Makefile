
all:
	$(MAKE) -C src/s3meta all

prefix ?= /usr/local

install: all
	install -d ${prefix}/bin
	install src/s3meta/s3meta ${prefix}/bin

clean:
	$(MAKE) -C src/s3meta clean


.PHONY: all install clean
