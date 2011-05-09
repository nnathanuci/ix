
include ../makefile.inc

all: librm.a rmtest rmtest_p14 rmtest_extra

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

# binary dependencies
rmtest: rmtest.o librm.a $(CODEROOT)/pf/libpf.a
	$(CC) -g3 -o rmtest rmtest.cc librm.a $(CODEROOT)/pf/libpf.a

# binary dependencies
rmtest_extra: rmtest_extra.o librm.a $(CODEROOT)/pf/libpf.a
	$(CC) -g3 -o rmtest_extra rmtest_extra.cc librm.a $(CODEROOT)/pf/libpf.a

rmtest_p14: rmtest_p14.cc rm.h librm.a $(CODEROOT)/pf/libpf.a
	$(CC) -g3 -o rmtest_p14 rmtest_p14.cc librm.a $(CODEROOT)/pf/libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

.PHONY: clean
clean:
	-rm test_insert rmtest_p14 test_tuple test_tuple.data rmtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
