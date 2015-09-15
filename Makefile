# Makefile for building the Debian distribution package containing the
# DataWareHouse connectiviy extension.

# author = Eric PASCUAL - CSTB (eric.pascual@cstb.fr)
# copyright = Copyright (c) 2015 CSTB
# version = 1.0.0

# name of the CSTBox module
MODULE_NAME=ext-dwh

include $(CSTBOX_DEVEL_HOME)/lib/makefile-dist.mk

copy_files: \
	copy_bin_files \
	copy_python_files \
	copy_etc_files \
	copy_init_scripts
