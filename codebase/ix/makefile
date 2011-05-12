
include ../makefile.inc

all: libix.a ixtest ixtest_ixmgr ixtest_ixhandle ixtest_ixscan

# lib file dependencies
libix.a: libix.a(ix.o ixmgr.o ixhandle.o ixscan.o IndexNode.o)

# c file dependencies
ix.o: ix.h

ixmgr.o: ix.h

ixhandle.o: ix.h

ixscan.o: ix.h

ixtest.o: ix.h

# binary dependencies
ixtest: ixtest.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

ixtest_ixmgr: ixtest_ixmgr.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

ixtest_ixscan: ixtest_ixscan.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

ixtest_ixhandle: ixtest_ixhandle.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a


.PHONY: $(CODEROOT)/pf/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

.PHONY: clean
clean:
	-rm ixtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
	$(MAKE) -C $(CODEROOT)/rm clean
