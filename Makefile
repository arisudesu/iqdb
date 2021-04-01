# Some configuration options
#----------------------------

# In simple mode, by default all data needed for queries is now
# read into memory, using in total about 500 bytes per image. It
# is possible to select a disk cache using mmap for this instead.
# Then the kernel can read this memory into the filecache or
# discard it as needed. The app uses as little memory as possible
# but depending on IO load queries can take longer (sometimes a lot).
# This option is especially useful for a VPS with little memory.
# DEFS+=-DUSE_DISK_CACHE

# If you do not have any databases created by previous versions of
# this software, you can uncomment this to not compile in code for
# upgrading old versions (use iqdb rehash <dbfile> to upgrade).
DEFS+=-DNO_SUPPORT_OLD_VER

# Enable a significantly less memory intensive but slightly slower
# method of storing the image index internally (in simple mode).
DEFS+=-DUSE_DELTA_QUEUE

# This may help or hurt performance. Try it and see for yourself.
DEFS+=-fomit-frame-pointer


.SUFFIXES:

all:	iqdb

%.o : %.h
%.o : %.cpp
iqdb.o : imgdb.h haar.h auto_clean.h
imgdb.o : imgdb.h imglib.h haar.h auto_clean.h delta_queue.h
test-db.o : imgdb.h
haar.o :
%.le.o : %.h
iqdb.le.o : imgdb.h haar.h auto_clean.h
imgdb.le.o : imgdb.h imglib.h haar.h auto_clean.h delta_queue.h
haar.le.o :

% : %.o haar.o imgdb.o # bloom_filter.o
	g++ -o $@ $^ ${CFLAGS} ${LDFLAGS} -g `pkg-config --libs ImageMagick` ${DEFS}

%.le : %.le.o haar.le.o imgdb.le.o # bloom_filter.le.o
	g++ -o $@ $^ ${CFLAGS} ${LDFLAGS} -g `pkg-config --libs ImageMagick` ${DEFS}

%.o : %.cpp
	g++ -c -o $@ $< -O2 -fpeel-loops ${CFLAGS} -DNDEBUG -Wall -DLinuxBuild -DImMagick -g `pkg-config --cflags ImageMagick` ${DEFS}

%.le.o : %.cpp
	g++ -c -o $@ $< -O2 -fpeel-loops ${CFLAGS} -DCONV_LE -DNDEBUG -Wall -DLinuxBuild -DImMagick -g `pkg-config --cflags ImageMagick` ${DEFS}
