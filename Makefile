#
# $Header$
#
# nsoracle --
#
#      Makefile for nsoracle database driver.
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
# Module Pretty-name (used to generalize the tag and file-release targets)
#
MODNAME  =  nsoracle

#
# Module name
#
MOD      =  nsoracle.so
MODCASS  =  nsoraclecass.so

#
# Objects to build
#
OBJS     =  nsoracle.o
OBJSCASS =  nsoraclecass.o

#
# Header files in THIS directory
#
HDRS     =  

#
# Extra libraries
#
OCI_VERSION=$(shell strings $(ORACLE_HOME)/lib/libclntsh.so | grep "^Version.[0-9]\+\.[0-9]")
OCI_MAJOR_VERSION=$(shell echo $(OCI_VERSION) | cut -d ' ' -f2 | cut -d '.' -f1)
NS_VERSION=$(shell grep NS_VERSION $(NSHOME)/include/ns.h)

MODLIBS  =  -L$(ORACLE_HOME)/lib -lclntsh \
	    -lcore$(OCI_MAJOR_VERSION) \
	    -lcommon$(OCI_MAJOR_VERSION) \
	    -lgeneric$(OCI_MAJOR_VERSION) \
	    -lclient$(OCI_MAJOR_VERSION)

ifneq (,$(findstring NS_VERSION,$(NS_VERSION)))
MODLIBS  +=  -lnsdb
endif

########################################################################
# Copied from Makefile.module because this module is a little more
# complicated.

# TODO: this should be possible to manage without maintaining a local copy of
# Makefile.module

include $(NSHOME)/include/Makefile.global

# Tack on the oracle includes after Makefile.global stomps CFLAGS
CFLAGS := -g \
    -I$(ORACLE_HOME)/rdbms/demo \
    -I$(ORACLE_HOME)/rdbms/public \
    -I$(ORACLE_HOME)/network/public \
    -I$(ORACLE_HOME)/plsql/public $(filter-out -Wconversion,$(CFLAGS))

all: $(MOD) $(MODCASS) 

$(MOD): $(OBJS)
	$(RM) $@
	$(LDSO) $(LDFLAGS) -o $@ $(OBJS) $(MODLIBS) $(LIBS)

$(MODCASS): $(OBJSCASS)
	$(RM) $@
	$(LDSO) $(LDFLAGS) -o $@ $(OBJSCASS) $(MODLIBS) $(LIBS)

$(OBJS): $(HDRS)

$(OBJSCASS): $(HDRS) nsoracle.c
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
# Help the poor developer
#
help:
	@echo "**" 
	@echo "** DEVELOPER HELP FOR THIS $(MODNAME)"
	@echo "**"
	@echo "** make tag VER=X.Y"
	@echo "**     Tags the module CVS code with the given tag."
	@echo "**     You can tag the CVS copy at any time, but follow the rules."
	@echo "**     VER must be of the form:"
	@echo "**         X.Y"
	@echo "**         X.YbetaN"
	@echo "**     You should browse CVS at SF to find the latest tag."
	@echo "**"
	@echo "** make file-release VER=X.Y"
	@echo "**     Checks out the code for the given tag from CVS."
	@echo "**     The result will be a releaseable tar.gz file of"
	@echo "**     the form: module-X.Y.tar.gz."
	@echo "**"

#
# Tag the code in CVS right now
#
tag:
	@if [ "$$VER" = "" ]; then echo 1>&2 "VER must be set to version number!"; exit 1; fi
	cvs rtag v$(VER_) $(MODNAME)

#
# Create a distribution file release
#
file-release:
	@if [ "$$VER" = "" ]; then echo 1>&2 "VER must be set to version number!"; exit 1; fi
	rm -rf work
	mkdir work
	cd work && cvs -d :pserver:anonymous@cvs.aolserver.sourceforge.net:/cvsroot/aolserver co -r v$(VER_) $(MODNAME)
	mv work/$(MODNAME) work/$(MODNAME)-$(VER)
	( cd work && tar cvf - $(MODNAME)-$(VER) ) | gzip -9 > $(MODNAME)-$(VER).tar.gz
	rm -rf work

