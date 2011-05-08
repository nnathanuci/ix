
- Modify the "CODEROOT" variable in makefile.inc to point to the root of your code base

- Copy your own implementation of PF component to folder "pf", and RM component to folder "rm".

- Implement the Index Manager:

   Go to folder "ix" and type in:

    make clean
    make
    ./ixtest

   The program should work.  But it does nothing.  You are supposed to implement the API of the index manager defined in ix.h

- By default you should not change those functions of the IX_Manager, IX_IndexHandle, and IX_IndexScan class defined in ix/ix.h. If you think some changes are really necessary, please contact us first.
