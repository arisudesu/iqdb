
.SUFFIXES:

all:	iqdb

%.o : %.h
%.o : %.cpp
iqdb.o : imgdb.h haar.h auto_clean.h
imgdb.o : imgdb.h imglib.h haar.h auto_clean.h
haar.o :
%.le.o : %.h
iqdb.le.o : imgdb.h haar.h auto_clean.h
imgdb.le.o : imgdb.h imglib.h haar.h auto_clean.h
haar.le.o :

% : %.o haar.o imgdb.o # bloom_filter.o
	g++ -o $@ $^ ${CFLAGS} ${LDFLAGS} -g `pkg-config --libs ImageMagick` ${DEFS}

%.le : %.le.o haar.le.o imgdb.le.o # bloom_filter.le.o
	g++ -o $@ $^ ${CFLAGS} ${LDFLAGS} -g `pkg-config --libs ImageMagick` ${DEFS}

%.o : %.cpp
	g++ -c -o $@ $< -O2 ${CFLAGS} -DNDEBUG -Wall -DLinuxBuild -DImMagick -g `pkg-config --cflags ImageMagick` ${DEFS}

%.le.o : %.cpp
	g++ -c -o $@ $< -O2 ${CFLAGS} -DCONV_LE -DNDEBUG -Wall -DLinuxBuild -DImMagick -g `pkg-config --cflags ImageMagick` ${DEFS}
