#
# $Header$
#
# ora8 --
#
#      Makefile for ora8 database driver.
#

ifdef INST
NSHOME ?= $(INST)
else
NSHOME ?= ../aolserver
endif

#
# Version number used in release tags. Valid VERs are "1.1c", "2.1", 
# "2.2beta7". VER "1.1c" will be translated into "v1_1c" by this Makefile.
# Usage: make file-release VER=1.1c
#
VER_ = $(subst .,_,$(VER))

#
# Module name
#
MOD      =  ora8.so
MODCASS  =  ora8cass.so

#
# Objects to build
#
OBJS     =  ora8.o
OBJSCASS =  ora8cass.o

#
# Header files in THIS directory
#
HDRS     =  

#
# Extra libraries
#
MODLIBS  =  -L$(ORACLE_HOME)/lib \
    -lclntsh -lcore8 -lcommon8 -lgeneric8 -lclient8

########################################################################
# Copied from Makefile.module because this module is a little more
# complicated.

# TODO: this should be possible to manage without maintaining a local copy of
# Makefile.module

include $(NSHOME)/include/Makefile.global

# Tack on the oracle includes after Makefile.global stomps CFLAGS
CFLAGS += \
    -I$(ORACLE_HOME)/rdbms/demo \
    -I$(ORACLE_HOME)/rdbms/public \
    -I$(ORACLE_HOME)/network/public \
    -I$(ORACLE_HOME)/plsql/public 

all: $(MOD) $(MODCASS)

# Override LIBS variable
LIBS=

$(MOD): $(OBJS)
	$(RM) $@
	$(LDSO) -o $@ $(OBJS) $(MODLIBS)

$(MODCASS): $(OBJSCASS)
	$(RM) $@
	$(LDSO) -o $@ $(OBJSCASS) $(MODLIBS)

$(OBJS): $(HDRS)

$(OBJSCASS): $(HDRS) ora8.c
	$(CC) $(CFLAGS) -DFOR_CASSANDRACLE=1 -o $@ -c $<

install: all
	$(RM) $(INSTBIN)/$(MOD)
	$(INSTALL_SH) $(MOD) $(INSTBIN)/
	$(RM) $(INSTBIN)/$(MODCASS)
	$(INSTALL_SH) $(MODCASS) $(INSTBIN)/

clean:
	$(RM) $(OBJS) $(MOD) $(OBJSCASS) $(MODCASS)

clobber: clean
	$(RM) *.so *.o *.a *~

distclean: clobber
	$(RM) TAGS core

#
# Create a distribution file release
#
file-release:
	@if [ "$$VER" = "" ]; then echo 1>&2 "VER must be set to version number!"; exit 1; fi
	rm -rf work
	mkdir work
	cd work && cvs -d :pserver:anonymous@cvs.aolserver.sourceforge.net:/cvsroot/aolserver co -r v$(VER_) nsoracle
	mv work/nsoracle work/nsoracle-$(VER)
	( cd work && tar cvf - nsoracle-$(VER) ) | gzip -9 > nsoracle-$(VER).tar.gz
	rm -rf work

