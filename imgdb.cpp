/***************************************************************************\
    imgdb.cpp - iqdb library implementation

    Copyright (C) 2008 piespy@gmail.com

    Originally based on imgSeek code, these portions
    Copyright (C) 2003 Ricardo Niederberger Cabral.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
\**************************************************************************/

/* System includes */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/mman.h>
#include <ctime> 

/* STL includes */
#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>

/* ImageMagick includes */
#include <magick/api.h>

/* iqdb includes */
#include "imgdb.h"
#include "imglib.h"

namespace imgdb {

// Globals
//keywordsMapType globalKwdsMap;
Score weights[2][6][3];

/* Fixed weight mask for pixel positions (i,j).
Each entry x = i*NUM_PIXELS + j, gets value max(i,j) saturated at 5.
To be treated as a constant.
 */
unsigned char imgBin[16384];
int imgBinInited = 0;

unsigned int random_bloom_seed = 0;
const static bool is_disk_db = true;
static size_t pageSize = 0;
static size_t pageMask = 0;
static size_t pageImgs = 0;
static size_t pageImgMask = 0;

/* Endianness */
#if CONV_LE && (__BIG_ENDIAN__ || _BIG_ENDIAN || BIG_ENDIAN)
#define FLIP(x) flip_endian(x)
#else
#define FLIP(x)
#endif

template<typename T>
void flip_endian(T& v) {
	//if (sizeof(T) != 4) throw fatal_error("Can't flip this endian.");
	union { T _v; char _c[sizeof(T)]; } c; c._v = v;
	for (char* one = c._c, * two = c._c + sizeof(T) - 1; one < two; one++, two--) {
		char t = *one; *one = *two; *two = t;
	}
	v = c._v;
};

//typedef std::priority_queue < KwdFrequencyStruct > kwdFreqPriorityQueue;

inline void imageIdIndex_map::unmap() {
	if (!m_base) return;
	if (munmap(m_base, m_length))
		fprintf(stderr, "WARNING: Could not unmap %zd bytes of memory.\n", m_length);
}

int tempfile() {
	char tempnam[] = "/tmp/imgdb_cache.XXXXXXX";
	int fd = mkstemp(tempnam);
	if (fd == -1) throw io_error("Can't open cache file.");
	if (unlink(tempnam))
		fprintf(stderr, "WARNING: Can't unlink cache file: %s.", strerror(errno));
	return fd;
}

template<bool is_simple>
int imageIdIndex_list<is_simple>::m_fd = -1;

template<bool is_simple>
void imageIdIndex_list<is_simple>::reserve(size_t s) {
	if (!is_simple) s = (s + pageImgMask) & ~pageImgMask;
	if (s <= m_capacity) return;
	if (m_fd == -1) m_fd = tempfile();
	size_t toadd = s - m_capacity;
	off_t page = lseek(m_fd, 0, SEEK_CUR);
//fprintf(stderr, "%zd/%zd entries at %llx=", toadd, s, page);
	m_baseofs = page & pageMask;
	size_t len = toadd * sizeof(imageId);
	m_capacity += len / sizeof(imageId);
	if (m_baseofs & (sizeof(imageId) - 1)) throw internal_error("Mis-aligned file position.");
	if (!is_simple && m_baseofs) throw internal_error("Base offset in write mode.");
	page &= ~pageMask;
//fprintf(stderr, "%llx:%zx, %zd bytes=%zd.\n", page, m_baseofs, len, m_capacity);
	if (ftruncate(m_fd, lseek(m_fd, len, SEEK_CUR))) throw io_error("Failed to resize bucket map file.");
	m_pages.push_back(imageIdPage(page, len));
}

template<bool is_simple>
imageIdIndex_map imageIdIndex_list<is_simple>::map_all(bool writable) {
	if (!writable && !size()) return imageIdIndex_map(NULL, NULL, 0, 0);
	if (!writable && m_pages.empty()) {
//fprintf(stderr, "Using fake map of tail data.\n");
		return imageIdIndex_map(NULL, &m_tail.front(), m_tail.size(), 0);
	}
	if (is_simple) {
		imageIdPage& page = m_pages.front();
		size_t length = page.second + m_baseofs;
//fprintf(stderr, "Directly mapping %zd bytes. ", length);
		void* base = mmap(NULL, length, PROT_READ|PROT_WRITE, MAP_SHARED, m_fd, page.first);
		if (base == MAP_FAILED) throw memory_error("Failed to mmap bucket.");
		return imageIdIndex_map(base, (image_id_index*)(((char*)base)+m_baseofs), m_size, length);
	}

	while (writable && !m_tail.empty()) page_out();

	size_t size = writable ? m_capacity : this->size();
	if (m_baseofs) throw internal_error("Base offset in write mode.");
	size_t len = size * sizeof(imageId);
	len = (len + pageMask) & ~pageMask;
//fprintf(stderr, "Making full map of %zd bytes. ", len);
	void* base = mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
	if (base == MAP_FAILED) throw memory_error("Failed to mmap bucket.");

	imageIdIndex_map mapret(base, (image_id_index*)base, size, len);
	char* chunk = (char*) base;
	for (page_list::iterator itr = m_pages.begin(); itr != m_pages.end(); ++itr) {
//fprintf(stderr, "Using %zd bytes from ofs %llx. ", itr->second, (long long int) itr->first);
		void* map = mmap(chunk, itr->second, PROT_READ|PROT_WRITE, MAP_FIXED|MAP_SHARED, m_fd, itr->first);
		if (map == MAP_FAILED) { mapret.unmap(); throw memory_error("Failed to mmap bucket chunk."); }
		chunk += itr->second;
	}
	size_t last = m_size & pageImgMask;
	if (last)
		chunk -= pageSize - last * sizeof(imageId);
//fprintf(stderr, "Last page has %zd, %zd bytes avail, ", last, pageSize - last * sizeof(imageId));
	image_id_index* tail = (image_id_index*) base;
	tail += m_size;
//fprintf(stderr, "Using %zd bytes from tail at %p/%p. ", m_tail.size() * sizeof(imageId), tail, chunk);
	memcpy(tail, &m_tail.front(), m_tail.size() * sizeof(imageId));
/*
	chunk += m_tail.size() * sizeof(imageId);
//fprintf(stderr, "Total=%d/%zd.\n", chunk - (char*)base, size);
	if (base != chunk - size * sizeof(imageId)) { mapret.unmap();
fprintf(stderr, "base=%p chunk=%p size=%zd(%zd/%zd) capacity=%zd.\n", base, chunk, size, m_tail.size(), m_size, m_capacity); 
 throw internal_error("Incorrect total page length."); }
*/
	return mapret;
}

template<bool is_simple>
void imageIdIndex_list<is_simple>::page_out() {
	if (is_simple) throw internal_error("Can't page out in simple DB mode.");
//fprintf(stderr, "Tail has %zd/%zd values. Paging out. ", m_tail.size(), size());
	size_t last = m_size & pageImgMask;
//fprintf(stderr, "Last page has %zd/%zd(%zd), ", last, m_size, m_capacity);
	if (!last) reserve(size());
	imageIdPage page = m_pages.back();
	if (((size() + pageImgMask) & ~pageImgMask) != m_capacity) {
//fprintf(stderr, "Need middle page for %zd(%zd)/%zd.", m_size, (size() + pageImgMask) & ~pageImgMask, m_capacity);
		size_t ofs = last;
		for (page_list::iterator itr = m_pages.begin(); itr != m_pages.end(); ++itr) {
			page = *itr;
//fprintf(stderr, " %zd@%llx", page.second / sizeof(imageId), (long long int) page.first);
			while (page.second && m_size > ofs) {
				ofs += pageImgs;
				page.first += pageSize;
				page.second -= pageSize;
			}
			if (ofs >= m_size) break;
		}
//fprintf(stderr, " -> %llx = %zd. ", (long long int) page.first, ofs);
		if (ofs != m_size) throw internal_error("Counted pages badly.");
	} else {
//fprintf(stderr, "map @%llx/%zd. ", (long long int) page.first + page.second - pageSize, page.second);
		page.first += page.second - pageSize;
	}

	image_id_index* ptr = (image_id_index*) mmap(NULL, pageSize, PROT_READ|PROT_WRITE, MAP_SHARED, m_fd, page.first);
	if (ptr == MAP_FAILED) throw memory_error("Failed to map tail page.");
	size_t copy = std::min(m_tail.size(), pageImgs - last);
//fprintf(stderr, "Fits %zd, ", copy);
	memcpy(ptr + last, &m_tail.front(), copy * sizeof(imageId));
	m_size += copy;
	if (copy == m_tail.size()) {
		m_tail.clear();
//fprintf(stderr, "has all. Now %zd.\n", size());
	} else {
		m_tail.erase(m_tail.begin(), m_tail.begin() + copy);
//fprintf(stderr, "Only fits %zd, %zd left = %zd.\n", copy, m_tail.size(), size());
	}
	if (munmap(ptr, pageSize))
		fprintf(stderr, "WARNING: Failed to munmap tail page.\n");
}

template<bool is_simple>
void imageIdIndex_list<is_simple>::remove(image_id_index i) {
	if (!size()) return;
//fprintf(stderr, "Removing %lx from %zd/%zd(%zd). ", i.id, m_tail.size(), m_size, m_capacity);
	image_list::iterator itr = std::find_if(m_tail.begin(), m_tail.end(), std::bind2nd(std::equal_to<image_id_index>(), i));
	if (itr != m_tail.end()) {
//fprintf(stderr, "Found in tail at %d/%zd.\n", itr - m_tail.begin(), m_tail.size());
		*itr = m_tail.back();
		m_tail.pop_back();
		return;
	}
	AutoImageIdIndex_map map(map_all(false));
	size_t ofs;
	for (ofs = 0; ofs < m_size && map[ofs] != i; ofs++);
	if (ofs < m_size) {
		if (map[ofs] != i) throw internal_error("Huh???");
//fprintf(stderr, "Found at %zd/%zd.\n", ofs, m_size);
		if (!m_tail.empty()) {
			map[ofs] = m_tail.back();
			m_tail.pop_back();
		} else {
			map[ofs] = map[--m_size];
		}
	}
//else fprintf(stderr, "NOT FOUND!!!\n");
}

template<bool is_simple>
void imageIdIndex_list<is_simple>::clear() {
	m_tail.clear();
	m_size = 0;
}

// Specializations accessing images as SigStruct* or size_t map, and imageIdIndex_map as imageId or index map.
template<> inline dbSpaceImpl<false>::imageIterator dbSpaceImpl<false>::image_begin() { return imageIterator(m_images.begin(), *this); }
template<> inline dbSpaceImpl<false>::imageIterator dbSpaceImpl<false>::image_end() { return imageIterator(m_images.end(), *this); }
template<> inline dbSpaceImpl<true>::imageIterator dbSpaceImpl<true>::image_begin() { return imageIterator(m_info.begin(), *this); }
template<> inline dbSpaceImpl<true>::imageIterator dbSpaceImpl<true>::image_end() { return imageIterator(m_info.end(), *this); }

template<bool is_simple>
inline typename dbSpaceImpl<is_simple>::imageIterator dbSpaceImpl<is_simple>::find(imageId i) { 
	map_iterator itr = m_images.find(i);
	if (itr == m_images.end()) throw param_error("Invalid image ID.");
	return imageIterator(itr, *this);
}

static const size_t no_cacheOfs = ~size_t();

void initImgBin()
{
	imgBinInited = 1;
	srand((unsigned)time(0)); 

	/* setup initial fixed weights that each coefficient represents */
	int i, j;

	/*
	0 1 2 3 4 5 6 i
	0 0 1 2 3 4 5 5 
	1 1 1 2 3 4 5 5
	2 2 2 2 3 4 5 5
	3 3 3 3 3 4 5 5
	4 4 4 4 4 4 5 5
	5 5 5 5 5 5 5 5
	5 5 5 5 5 5 5 5
	j
	 */

	/* Every position has value 5, */
	memset(imgBin, 5, NUM_PIXELS_SQUARED);

	/* Except for the 5 by 5 upper-left quadrant: */
	for (i = 0; i < 5; i++)
		for (j = 0; j < 5; j++)
			imgBin[i * 128 + j] = std::max(i, j);
	// Note: imgBin[0] == 0

	/* Integer weights. */
	for (i = 0; i < 2; i++)
		for (j = 0; j < 6; j++)
			for (int c = 0; c < 3; c++)
				weights[i][j][c] = lrint(weightsf[i][j][c] * ScoreMax);
}

template<bool is_simple>
bool dbSpaceImpl<is_simple>::hasImage(imageId id) {
	if (is_simple && m_sigFile == -1) throw usage_error("Not supported in simple mode.");
	return m_images.find(id) != m_images.end();
}

template<bool is_simple>
inline ImgData dbSpaceImpl<is_simple>::get_sig_from_cache(imageId id) {
	if (is_simple && m_sigFile == -1) throw usage_error("Not supported in simple mode.");
	ImgData sig;
	read_sig_cache(find(id).cOfs(), &sig);
	return sig;
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::getImageWidth(imageId id) {
	return find(id).width();
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::getImageHeight(imageId id) {
	return find(id).height();
}

inline bool is_grayscale(const lumin_int& avgl) {
	return abs(avgl[1]) + abs(avgl[2]) < ScoreMax * 6 / 1000;
}

template<bool is_simple>
bool dbSpaceImpl<is_simple>::isImageGrayscale(imageId id) {
	return is_grayscale(find(id).avgl());
}

template<bool is_simple>
const lumin_int& dbSpaceImpl<is_simple>::getImageAvgl(imageId id) {
	return find(id).avgl();
}

void sigFromImage(Image* image, imageId id, ImgData* sig) {
	// Made static for speed; only used locally
	static Unit cdata1[16384];
	static Unit cdata2[16384];
	static Unit cdata3[16384];

	AutoExceptionInfo exception;

	/*
	Initialize the image info structure and read an image.
	 */

	sig->id = id;
	sig->width = image->columns;
	sig->height = image->rows;

	//resize_image = SampleImage(image, 128, 128, &exception);
	AutoImage resize_image(ResizeImage(image, 128, 128, TriangleFilter, 1.0, &exception));
	if (!resize_image)
		throw image_error("Unable to resize image.");

	unsigned char rchan[16384];
	unsigned char gchan[16384];
	unsigned char bchan[16384];

	const PixelPacket *pixel_cache = AcquireImagePixels(&resize_image, 0, 0, 128, 128, &exception);

	for (int idx = 0; idx < 16384; idx++) {
		rchan[idx] = pixel_cache->red >> (QuantumDepth - 8);
		gchan[idx] = pixel_cache->green >> (QuantumDepth - 8);
		bchan[idx] = pixel_cache->blue >> (QuantumDepth - 8);
		pixel_cache++;
	}

	transformChar(rchan, gchan, bchan, cdata1, cdata2, cdata3);

	calcHaar(cdata1, cdata2, cdata3, sig->sig1, sig->sig2, sig->sig3, sig->avglf);
}

template<bool is_simple>
void dbSpaceImpl<is_simple>::addSigToBuckets(ImgData* nsig) {
	if (is_simple) throw usage_error("Invalid for read-only db.");

	lumin_int avgl;
	SigStruct::avglf2i(nsig->avglf, avgl);
	for (int i = 0; i < NUM_COEFS; i++) {	// populate buckets


#ifdef FAST_POW_GEERT
		int x, t;
		// sig[i] never 0
		int x, t;

		x = nsig->sig1[i];
		t = (x < 0);		/* t = 1 if x neg else 0 */
		/* x - 0 ^ 0 = x; i - 1 ^ 0b111..1111 = 2-compl(x) = -x */
		x = (x - t) ^ -t;
		imgbuckets[0][t][x].push_back(nsig->id);

		if (is_grayscale(avg))
			continue;	// ignore I/Q coeff's if chrominance too low

		x = nsig->sig2[i];
		t = (x < 0);
		x = (x - t) ^ -t;
		imgbuckets[1][t][x].push_back(nsig->id);

		x = nsig->sig3[i];
		t = (x < 0);
		x = (x - t) ^ -t;
		imgbuckets[2][t][x].push_back(nsig->id);

		should not fail

#else //FAST_POW_GEERT
		//imageId_array3 imgbuckets = dbSpace[dbId]->imgbuckets;
		if (nsig->sig1[i]>0) imgbuckets[0][0][nsig->sig1[i]].push_back(nsig->id);
		if (nsig->sig1[i]<0) imgbuckets[0][1][-nsig->sig1[i]].push_back(nsig->id);

		if (is_grayscale(avgl))
			continue;	// ignore I/Q coeff's if chrominance too low

		if (nsig->sig2[i]>0) imgbuckets[1][0][nsig->sig2[i]].push_back(nsig->id);
		if (nsig->sig2[i]<0) imgbuckets[1][1][-nsig->sig2[i]].push_back(nsig->id);

		if (nsig->sig3[i]>0) imgbuckets[2][0][nsig->sig3[i]].push_back(nsig->id);
		if (nsig->sig3[i]<0) imgbuckets[2][1][-nsig->sig3[i]].push_back(nsig->id);   

#endif //FAST_POW_GEERT

	}
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::addImageFromImage(imageId id, Image* image) {
	/* id is a unique image identifier
	filename is the image location
	thname is the thumbnail location for this image
	doThumb should be set to 1 if you want to save the thumbnail on thname
	Images with a dimension smaller than ignDim are ignored
	 */

	if (is_simple) throw usage_error("Not valid in read-only DB mode.");

	ImgData sig;
	sigFromImage(image, id, &sig);

	SigStruct *nsig = new SigStruct(get_sig_cache());
	nsig->init(&sig);
	write_sig_cache(nsig->cacheOfs, &sig);

	// insert into sigmap
	m_images.add_sig(id, nsig);
	// insert into ids bloom filter
	//imgIdsFilter->insert(id);

	addSigToBuckets(&sig);

	// success after all
	return 1;
}

inline void check_image(Image* image) {
	if (!image)	// unable to read image
		throw image_error("Unable to read image data.");

	if (image->colorspace != RGBColorspace)
		throw image_error("Invalid color space.");
	if (image->storage_class != DirectClass) {
		SyncImage(image);
		SetImageType(image, TrueColorType);
		SyncImage(image);
	}
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::addImageBlob(imageId id, const void *blob, size_t length) {
	if (hasImage(id)) // image already in db
		throw param_error("Image already in database.");

	AutoExceptionInfo exception;
	AutoImageInfo image_info;
	AutoImage image(BlobToImage(&image_info, blob, length, &exception));
	if (exception.severity != UndefinedException) CatchException(&exception);
	check_image(&image);
	return addImageFromImage(id, &image);//, thname, doThumb, ignDim);
}

Image* imageFromFile(const char* filename) {
	AutoExceptionInfo exception;
	AutoImageInfo image_info;

	strcpy(image_info->filename, filename);
	Image* image = ReadImage(&image_info, &exception);
	if (exception.severity != UndefinedException) CatchException(&exception);
	check_image(image);
	return image;
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::addImage(imageId id, const char *filename) {
	if (hasImage(id)) // image already in db
		throw param_error("Image already in database.");

	AutoImage image(imageFromFile(filename));
	return addImageFromImage(id, &image);
}

void dbSpace::imgDataFromFile(const char* filename, ImgData* img) {
	AutoImage image(imageFromFile(filename));
	sigFromImage(&image, 0, img);
}

template<>
void dbSpaceImpl<false>::setImageRes(imageId id, int width, int height) {
	imageIterator itr = find(id);
	ImgData sig;
	read_sig_cache(itr.cOfs(), &sig);
	sig.width = width;
	sig.height = height;
	write_sig_cache(itr.cOfs(), &sig);
}
template<> void dbSpaceImpl<true>::setImageRes(imageId id, int width, int height) { throw usage_error("Not valid in read-only mode."); }

template<bool is_simple>
void dbSpaceImpl<is_simple>::load_stream(std::ifstream& f, srzMetaDataStruct& md) {
fprintf(stderr, "Loading db... ");

	imageId id;
	unsigned int sz;	

	if (md.iskVersion < SRZ_V0_6_0) {
		throw data_error("Database from a version prior to 0.6");
	} else if (md.iskVersion > SRZ_V0_6_1) {
		throw data_error("Database from a version after 0.6.1");
	}

	// fast-forward to image count
	std::ifstream::pos_type old_pos = f.tellg();
	for (int c = 0; c < 3; c++)
		for (int pn = 0; pn < 2; pn++)
			for (int i = 0; i < 16384; i++) {
				f.read((char *) &(sz), sizeof(int));
				FLIP(sz);
				f.seekg(sz * sizeof(imageId), f.cur);
			}
	typename image_map::size_type szt;
	f.read((char *) &(szt), sizeof(szt));
	FLIP(szt);
	f.seekg(old_pos);
fprintf(stderr, "has %zd images. ", szt);
	//To discard most common buckets: szt /= 10;

	// read buckets
	for (int c = 0; c < 3; c++)
		for (int pn = 0; pn < 2; pn++)
			for (int i = 0; i < 16384; i++) {
				f.read((char *) &(sz), sizeof(int));
				FLIP(sz);
				if (!sz) continue;

				imageIdIndex_list<is_simple>& bucket = imgbuckets[c][pn][i];

				if (is_simple && sz > szt) {
//fprintf(stderr, "Bucket %d:%d:%d has %d/%zd, ignoring.\n", c, pn, i, sz, szt*10);
					f.seekg(sz * sizeof(imageId), f.cur);
					continue;
				}

//fprintf(stdout, "c=%d:p=%d:%d=%d images\n", c, pn, i, sz);
				size_t szp = is_simple ? sz : sz & ~pageImgMask;
				if (szp) {
					if (sz - szp >= imageIdIndex_list<is_simple>::threshold) szp = sz;
					bucket.reserve(szp);
					AutoImageIdIndex_map map(bucket.map_all(true));
//fprintf(stderr, "Mapped bucket to %d bytes at %p(%p). Reading to %p. ", map.m_length, map.m_base, map.m_img, &(map[0]));
					f.read((char *) &(map[0]), szp * sizeof(imageId));
					for (size_t k = 0; k < szp; k++)
						FLIP(map[k].id);
					bucket.loaded(szp);
					sz -= szp;
				}
				for (size_t k = 0; k < sz; k++) {
					f.read((char *) &id, sizeof(imageId));
					FLIP(id);
					bucket.push_back(id);
				}
//fprintf(stderr, "Bucket has %zd capacity paged, %zd in pages, %zd in tail.\n", bucket.paged_capacity(), bucket.paged_size(), bucket.tail_size());
//fprintf(stderr, "Done.\n");
			}
fprintf(stderr, "buckets done... ");

	// read sigs
	f.read((char *) &(szt), sizeof(szt));
	FLIP(szt);

	if (md.iskVersion == SRZ_V0_6_0)
fprintf(stderr, "converting from v6.0... ");

	// current version
	if (is_simple) m_info.resize(szt);

		for (typename image_map::size_type k = 0; k < szt; k++) {
			ImgData sig;

			if (md.iskVersion == SRZ_V0_6_0) {
				Idx* sigs[3] = { sig.sig1, sig.sig2, sig.sig3 };
				f.read((char *) &sig.id, sizeof(sig.id));
				for (int s = 0; s < 3; s++) for (int i = 0; i < NUM_COEFS; i++) {
					int idx;
					f.read((char *) &idx, sizeof(idx));
					sigs[s][i] = idx;
				}
				f.read((char *) sig.avglf, sizeof(sig.avglf));
				f.read((char *) &sig.width, sizeof(sig.width));
				f.read((char *) &sig.height, sizeof(sig.height));
			} else {
				f.read((char *) &sig, sizeof(ImgData));
			}
			FLIP(sig.id); FLIP(sig.width); FLIP(sig.height); FLIP(sig.avglf[0]); FLIP(sig.avglf[1]); FLIP(sig.avglf[2]);
			size_t ind = m_nextIndex++;
			if (is_simple) {
				m_info[ind].id = sig.id;
				SigStruct::avglf2i(sig.avglf, m_info[ind].avgl);
				m_info[ind].width = sig.width;
				m_info[ind].height = sig.height;
				m_images.add_index(sig.id, ind);

				if (m_sigFile == -1) {
					int szk;
					f.read((char *) &(szk), sizeof(int));
					FLIP(szk);
					if (szk) throw data_error("Can't use keywords in simple DB mode.");
					continue;
				}

				size_t ofs = get_sig_cache();
				if (ofs != ind * sizeof(ImgData)) throw internal_error("Index and cache out of sync!");
				write_sig_cache(ofs, &sig);

			} else {
				SigStruct* nsig = new SigStruct(get_sig_cache());
				nsig->init(&sig);
				nsig->index = ind;
				write_sig_cache(nsig->cacheOfs, &sig);

				// insert new sig
				m_images.add_sig(sig.id, nsig);
			}
			// insert into ids bloom filter
			// imgIdsFilter->insert(sig.id);
			// read kwds
			int szk;
			f.read((char *) &(szk), sizeof(int));
			FLIP(szk);
			if (szk) throw data_error("Keywords not supported.");

			/*
			int kwid;
			if (szk && !nsig->keywords) nsig->keywords = new int_hashset;
			for (int ki = 0; ki < szk; ki++) {
				f.read((char *) &(kwid), sizeof(int));
				FLIP(kwid);
				nsig->keywords->insert(kwid);
				// populate keyword postings
				getKwdPostings(kwid)->imgIdsFilter->insert(nsig->id);
			}
			*/
		}

	if (is_simple) {
if (is_disk_db) fprintf(stderr, "map size: %lld... ", (long long int) lseek(imageIdIndex_list<is_simple>::fd(), 0, SEEK_CUR));
fprintf(stderr, "convert to index... ");
		for (int c = 0; c < 3; c++)
			for (int pn = 0; pn < 2; pn++)
				for (int i = 0; i < 16384; i++) {
					imageIdIndex_list<is_simple>& bucket = imgbuckets[c][pn][i];

					AutoImageIdIndex_map map(bucket.map_all(false));
					for (imageIdIndex_map::iterator itr = map.begin(); itr != map.end(); ++itr)
						itr->index = find(itr->id).index();
				}
//fprintf(stderr, "%d:%d:%d=%d buckets\n", c, pn, i, sz);
		if (m_sigFile == -1) m_images.clear();
	}
	m_bucketsValid = true;
fprintf(stderr, "complete!\n");
}

srzMetaDataStruct loadGlobalSerializationMetadata(std::ifstream& f) {
fprintf(stderr, "Reading metadata.\n");

	srzMetaDataStruct md;

	// isk version
	f.read((char *) &(md.iskVersion), sizeof(int));
	FLIP(md.iskVersion);

	// binding language
	f.read((char *) &(md.bindingLang), sizeof(int));
	FLIP(md.bindingLang);

	// trial or full
	f.read((char *) &(md.isTrial), sizeof(int));
	FLIP(md.isTrial);

	// platform
	f.read((char *) &(md.compilePlat), sizeof(int));
	FLIP(md.compilePlat);

	// ok, I have some valid metadata
	md.isValidMetadata = 1;

	return md;
}

inline dbSpace* make_dbSpace(int mode) {
	return  mode != dbSpace::mode_normal
		? static_cast<dbSpace*>(new dbSpaceImpl<true>(mode != dbSpace::mode_simple))
		: static_cast<dbSpace*>(new dbSpaceImpl<false>(true));
};

dbSpace* dbSpace::load_file(const char *filename, int mode) {
	dbSpace* db = make_dbSpace(mode);

	std::ifstream f(filename, std::ios::binary);
	if (!f.is_open()) {
		fprintf(stderr, "ERROR: unable to open file %s for read ops: %s.\n", filename, strerror(errno));
		return db;
	}

	srzMetaDataStruct md;
	md.isValidMetadata = 0;

	int isMetadata;
	f.read((char *) &(isMetadata), sizeof(int));
	FLIP(isMetadata);
	if (!isMetadata) throw data_error("No metadata.");

	md = loadGlobalSerializationMetadata(f);		

	db->load_stream(f, md);

	f.close();
	return db;
}

dbSpaceMap dbSpaceMap::load_file(const char* filename, int mode) {
	std::ifstream f(filename, std::ios::binary);
	if (!f.is_open()) { // file not found, perhaps its the first start
		fprintf(stderr, "ERROR: unable to open file %s for read ops: %s.\n", filename, strerror(errno));
		return dbSpaceMap();
	}

	unsigned int isMetadata = 0;

	srzMetaDataStruct md;
	md.isValidMetadata = 0;

	isMetadata = SRZ_VERSIONED;
	if (isMetadata == SRZ_VERSIONED) {// has metadata
		f.read((char *) &(isMetadata), sizeof(int));
		FLIP(isMetadata);
		if (isMetadata != SRZ_VERSIONED) {
			fprintf(stderr, "ERROR: Not a multiple database file.\n");
			return dbSpaceMap();
		}
		md = loadGlobalSerializationMetadata(f);
	}

	int dbId = 1;
	int sz = 0;

	f.read((char *) &(sz), sizeof(int)); // number of dbs
	FLIP(sz);

fprintf(stderr, "Reading %d databases...\n", sz);
	dbSpaceMap ret;
	for (int k = 0; k < sz; k++) { // for each db
		f.read((char *) &(dbId), sizeof(int)); // db id
		FLIP(dbId);
fprintf(stderr, "Reading DB %d\n", dbId);
		dbSpace* db = make_dbSpace(mode);
		db->load_stream(f, md);
		ret[dbId] = db;
	}

	f.close();
	return ret;
}

template<>
void dbSpaceImpl<false>::save_stream(std::ofstream& f) {
	/*
	Serialization order:
	for each color {0,1,2}:
	for {positive,negative}:
	for each 128x128 coefficient {0-16384}:
	[int] bucket size (size of list of ids)
	for each id:
	[imageId] image id
	[int] number of images (signatures)
	for each image:
	[long id] image id
	for each sig coef {0-39}:  (the NUM_COEFS greatest coefs)
	for each color {0,1,2}:
	[int] coef index (signed)
	for each color {0,1,2}:
	[double] average luminance
	[int] image width
	[int] image height

	 */

	if (!m_bucketsValid) throw usage_error("Can't save with invalid buckets.");
if (is_disk_db) fprintf(stderr, "map size: %lld... ", (long long int) lseek(imgbuckets[0][0][0].fd(), 0, SEEK_CUR));

	// save buckets
	for (int c = 0; c < 3; c++) {
		for (int pn = 0; pn < 2; pn++) {
			for (int i = 0; i < 16384; i++) {
				imageIdIndex_list<false>& bucket = imgbuckets[c][pn][i];

				int sz = bucket.size();
//fprintf(stderr, "%d:%d:%d=%d buckets\n", c, pn, i, sz);
				AutoImageIdIndex_map map(bucket.map_all(false));

				f.write((char *) &(sz), sizeof(int));
				f.write((char *) &(map[0]), sz * sizeof(imageId));
			}
		}
	}

	// save sigs
	image_map::size_type szt = m_images.size();

	f.write((char *) &(szt), sizeof(szt));

	for (imageIterator it = image_begin(); it != image_end(); it++) {
		ImgData dsig;
		read_sig_cache(it.cOfs(), &dsig);
		f.write((char *) &dsig, sizeof(ImgData));

		//// keywords
		// if (!sig->keywords) {
			int sz = 0;
			f.write((char *) &(sz), sizeof(int));
			continue;
		/*
		}
		int_hashset& kwds = *sig->keywords;
		sz = kwds.size();
		// number of keywords
		f.write((char *) &(sz), sizeof(int));
		// dump keywds
		int kwid;
		for (int_hashset::iterator itkw = kwds.begin(); itkw != kwds.end(); itkw++) {
			kwid = *itkw;
			f.write((char *) &(kwid), sizeof(int));
		}
		*/		
	}
}

template<> void dbSpaceImpl<true>::save_stream(std::ofstream& f) { throw usage_error("Can't save read-only db."); }

void saveGlobalSerializationMetadata(std::ofstream& f) {
fprintf(stderr, "Saving metadata.\n");

	int wval;

	// is versioned
	wval = SRZ_VERSIONED;
	f.write((char*)&(wval), sizeof(int));

	// isk version
	wval = SRZ_V0_6_1;
	f.write((char*)&(wval), sizeof(int));

	// binding language
#ifdef ISK_SWIG_JAVA
	wval = SRZ_LANG_JAVA;
	f.write((char*)&(wval), sizeof(int));	
#else	
	wval = SRZ_LANG_PYTHON;
	f.write((char*)&(wval), sizeof(int));	
#endif

	// trial or full
		wval = SRZ_FULL_VERSION;
		f.write((char*)&(wval), sizeof(int));	

	// platform	
#ifdef _WINDOWS
	wval = SRZ_PLAT_WINDOWS;	
	f.write((char*)&(wval), sizeof(int));	
#else
	wval = SRZ_PLAT_LINUX;
	f.write((char*)&(wval), sizeof(int));	
#endif
}

template<bool is_simple>
int dbSpaceImpl<is_simple>::save_file(const char *filename) {
	std::string temp = std::string(filename) + ".temp";
	std::ofstream f(temp.c_str(), std::ios::binary);
	if (!f.is_open()) {
		fprintf(stderr, "ERROR: error opening file %s for write ops: %s\n", temp.c_str(), strerror(errno));
		return 0;			
	}

	saveGlobalSerializationMetadata(f);

	save_stream(f);
	f.close();
	if (rename(temp.c_str(), filename)) {
		fprintf(stderr, "ERROR: Can't rename %s to %s: %s\n", temp.c_str(), filename, strerror(errno));
		return 0;
	}
	return 1;
}

int dbSpaceMap::save_file(const char* filename) {
	std::string temp = std::string(filename) + ".temp";
	std::ofstream f(temp.c_str(), std::ios::binary);
	if (!f.is_open()) {
		fprintf(stderr, "ERROR: error opening file %s for write ops: %s\n", temp.c_str(), strerror(errno));
		return 0;			
	}

	saveGlobalSerializationMetadata(f);

	int sz = size();
	f.write((char *) &(sz), sizeof(int)); // num dbs
	int dbId;

	for (iterator it = begin(); it != end(); it++) {
		dbId = (*it).first;
		f.write((char *) &(dbId), sizeof(int)); // db id
fprintf(stderr, "Saving DB %d\n", dbId);
		it->second->save_stream(f);
	}

	f.close();
	if (rename(temp.c_str(), filename)) {
		fprintf(stderr, "ERROR: Can't rename %s to %s: %s\n", temp.c_str(), filename, strerror(errno));
		return 0;
	}
	return size();
}

template<bool is_simple>
struct sim_result : public index_iterator<is_simple>::base_type {
	typedef typename index_iterator<is_simple>::base_type itr_type;
	sim_result(Score s, const itr_type& i) : itr_type(i), score(s) { }
	bool operator< (const sim_result& other) const { return score < other.score; }
	Score score;
};

template<bool is_simple>
template<int num_colors>
sim_vector dbSpaceImpl<is_simple>::do_query(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags, bloom_filter* bfilter, bool fast) {
	int idx, c;
	int pn;
	const Idx *sig[3] = { sig1, sig2, sig3 };
	Score scale = 0;
	int sketch = flags & flag_sketch ? 1 : 0;
//fprintf(stderr, "In do_query<%s,%d>.\n", is_simple?"true":"false", num_colors);

	if (!m_bucketsValid) throw usage_error("Can't query with invalid buckets.");

	size_t count = m_nextIndex;
	Score* scores = new Score[count];

	if (bfilter) { // make sure images not on filter are penalized
		throw usage_error("Filtering not supported.");
		/*
		for (sigMap::iterator sit = sigs.begin(); sit != sigs.end(); sit++) {

			if (!bfilter->contains((*sit).first)) { // image doesnt have keyword, just give it a terrible score
				scores[sit->second->get_index] = ScoreMax;
			} else { // ok, image content should be taken into account
				for (c = 0; c < cnum; c++) {
					scores[sit->second->get_index] += (((DScore)weights[sketch][0][c]) * abs(info[sit->second->get_index].second[c] - avgl[c])) >> ScoreScale;
				}
			}
		}
		delete bfilter;
		*/

	} else { // search all images 
		for (imageIterator itr = image_begin(); itr != image_end(); ++itr) {
			scores[itr.index()] = 0;
			for (c = 0; c < num_colors; c++)
				scores[itr.index()] += (((DScore)weights[sketch][0][c]) * abs(itr.avgl()[c] - avgl[c])) >> ScoreScale;
		}
	}

	for (int b = fast ? NUM_COEFS : 0; b < NUM_COEFS; b++) {	// for every coef on a sig
		for (c = 0; c < num_colors; c++) {
			//TODO see if FAST_POW_GEERT gives the same results			
#ifdef FAST_POW_GEERT    	
			pn  = sig[c][b] < 0;
			idx = (sig[c][b] - pn) ^ -pn;
#else
			pn = 0;
			if (sig[c][b]>0) {
				pn = 0;
				idx = sig[c][b];
			} else {
				pn = 1;
				idx = -sig[c][b];
			}
#endif
			imageIdIndex_list<is_simple>& bucket = imgbuckets[c][pn][idx];
			if (bucket.empty()) continue;
			if (flags & flag_nocommon && bucket.size() > count / 10) continue;

			Score weight = weights[sketch][imgBin[idx]][c]; 
//fprintf(stderr, "%d:%d=%d has %zd=%d, ", b, c, sig[c][b], dbSpace[dbId]->imgbuckets[c][pn][idx].size(), weight);
			scale -= weight;

			// update the score of every image which has this coef
			AutoImageIdIndex_map map(bucket.map_all(false));
			for (idIndexIterator itr(map.begin(), *this); itr != map.end(); ++itr)
				scores[itr.index()] -= weight;
		}
	}

	typedef std::priority_queue<sim_result<is_simple> > sigPriorityQueue;

	sigPriorityQueue pqResults;		/* results priority queue; largest at top */

	imageIterator itr = image_begin();

	sim_vector V;

	typedef std::map<int, size_t> set_map;
	set_map sets;
	unsigned int need = numres;

	// Fill up the numres-bounded priority queue (largest at top):
	for (unsigned int cnt = 0; cnt < need && itr != image_end(); cnt++, ++itr) {
		pqResults.push(sim_result<is_simple>(scores[itr.index()], itr));

		if (flags & flag_uniqueset) //{
			need += ++sets[itr.set()] > 1;
//imageIterator top(pqResults.top(), *this);fprintf(stderr, "Added id=%08lx score=%.2f set=%x, now need %d. Worst is id %08lx score %.2f set %x has %zd\n", itr.id(), (double)scores[itr.index()]/ScoreMax, itr.set(), need, top.id(), (double)pqResults.top().score/ScoreMax, top.set(), sets[top.set()]); }
	}

	for (; itr != image_end(); ++itr) {
		// only consider if not ignored due to keywords and if is a better match than the current worst match
		if (scores[itr.index()] < pqResults.top().score) {
			// Make room by dropping largest entry:
			if (flags & flag_uniqueset) {
				pqResults.push(sim_result<is_simple>(scores[itr.index()], itr));
				need += ++sets[itr.set()] > 1;
//imageIterator top(pqResults.top(), *this);fprintf(stderr, "Added id=%08lx score=%.2f set=%x, now need %d. Worst is id %08lx score %.2f set %x has %zd\n", itr.id(), (double)scores[itr.index()]/ScoreMax, itr.set(), need, top.id(), (double)pqResults.top().score/ScoreMax, top.set(), sets[top.set()]);

				while (pqResults.size() > need || sets[imageIterator(pqResults.top(), *this).set()] > 1) {
					need -= sets[imageIterator(pqResults.top(), *this).set()]-- > 1;
					pqResults.pop();
//imageIterator top(pqResults.top(), *this);fprintf(stderr, "Dropped worst, now need %d. New worst is id %08lx score %.2f set %x has %zd\n", need, top.id(), (double)pqResults.top().score/ScoreMax, top.set(), sets[top.set()]);
				}
			} else {
				pqResults.pop();
				// Insert new entry:
				pqResults.push(sim_result<is_simple>(scores[itr.index()], itr));
			}
		}
	}

	delete[] scores;

	scale = ((DScore) ScoreMax) * ScoreMax / scale;
	while (!pqResults.empty()) {
		const sim_result<is_simple>& curResTmp = pqResults.top();
		if (curResTmp.score == ScoreMax)
			break;

		imageIterator itr(curResTmp, *this);
//fprintf(stderr, "Candidate %08lx = %.2f, set %x has %zd.\n", itr.id(), (double)curResTmp.score/ScoreMax, itr.set(), sets[itr.set()]);
		if (!(flags && flag_uniqueset) || sets[itr.set()]-- < 2)
			V.push_back(sim_value(itr.id(), (((DScore)curResTmp.score) * 100 * scale) >> ScoreScale, itr.width(), itr.height()));
//else fprintf(stderr, "Skipped!\n");
		pqResults.pop();
	}

	std::reverse(V.begin(), V.end());
	return V;

}

template<bool is_simple>
inline sim_vector
dbSpaceImpl<is_simple>::queryImgDataFiltered(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags, bloom_filter* bfilter, bool fast) {
	if ((flags & flag_grayscale) || is_grayscale(avgl))
		return do_query<1>(sig1, sig2, sig3, avgl, numres, flags, bfilter, fast);
	else
		return do_query<3>(sig1, sig2, sig3, avgl, numres, flags, bfilter, fast);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgDataFiltered(const ImgData& img, unsigned int numres, int flags, bloom_filter* bfilter, bool fast) {
	lumin_int avgl;
	SigStruct::avglf2i(img.avglf, avgl);
	return queryImgDataFiltered(img.sig1, img.sig2, img.sig3, avgl, numres, flags, bfilter, fast);
}

/* sig1,2,3 are int arrays of length NUM_COEFS 
avgl is the average luminance
numres is the max number of results
sketch (0 or 1) tells which set of weights to use
 */
template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgData(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags) {
	return queryImgDataFiltered(sig1, sig2, sig3, avgl, numres, flags, 0, false);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgData(const ImgData& img, unsigned int numres, int flags) {
	lumin_int avgl;
	SigStruct::avglf2i(img.avglf, avgl);
	return queryImgData(img.sig1, img.sig2, img.sig3, avgl, numres, flags);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgFile(const char* filename, unsigned int numres, int flags) {
	AutoImage image(imageFromFile(filename));
fprintf(stderr, "Loaded image %s... ", filename);
	ImgData sig;
	sigFromImage(&image, 0, &sig);
fprintf(stderr, "got sig, flags = %d.\n", flags);
	return queryImgDataFiltered(sig, numres, flags, 0, false);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgDataFast(const ImgData& img, unsigned int numres, int flags) {
	lumin_int avgl;
	SigStruct::avglf2i(img.avglf, avgl);
	return queryImgDataFiltered(img.sig1, img.sig2, img.sig3, avgl, numres, flags, NULL, true);
}

// cluster by similarity. Returns list of list of imageIds (img ids)
/*
imageId_list_2 clusterSim(const int dbId, float thresd, int fast = 0) {
	imageId_list_2 res;		// will hold a list of lists. ie. a list of clusters
	sigMap wSigs(dbSpace[dbId]->sigs);		// temporary map of sigs, as soon as an image becomes part of a cluster, it's removed from this map
	sigMap wSigsTrack(dbSpace[dbId]->sigs);	// temporary map of sigs, as soon as an image becomes part of a cluster, it's removed from this map

	for (sigIterator sit = wSigs.begin(); sit != wSigs.end(); sit++) {	// for every img on db
		imageId_list res2;

		if (fast) {
			res2 =
				queryImgDataForThresFast(&wSigs, (*sit).second->avgl,
				thresd, 1);
		} else {
			res2 =
				queryImgDataForThres(dbId, &wSigs, (*sit).second->sig1,
				(*sit).second->sig2,
				(*sit).second->sig3,
				(*sit).second->avgl, thresd, 1);
		}
		//    continue;
		imageId hid = (*sit).second->id;
		//    if () 
		wSigs.erase(hid);
		if (res2.size() <= 1) {
			if (wSigs.size() <= 1)
				break;		// everything already added to a cluster sim. Bail out immediately, otherwise next iteration 
			// will segfault when incrementing sit
			continue;
		}
		res2.push_front(hid);
		res.push_back(res2);
		if (wSigs.size() <= 1)
			break;
		// sigIterator sit2 = wSigs.end();
		//    sigIterator sit3 = sit++;
	}
	return res;
}
 */

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgRandom(unsigned int numres) {
	/*query for images similar to the one that has this id
	numres is the maximum number of results
	 */
	throw usage_error("Not supported.");
	/*

		sim_vector Vres;
		size_t sz = validate_dbid(dbId)->sigs.size();
		int_hashset includedIds;
		sigIterator it = dbSpace[dbId]->sigs.begin();
		for (size_t var = 0; var < std::min<size_t>(sz, numres); ) { // var goes from 0 to numres
			size_t rint = rand()%(sz);
			for(size_t pqp =0; pqp < rint; pqp++) {
				it ++;			
				if (it == dbSpace[dbId]->sigs.end()) {
					it = dbSpace[dbId]->sigs.begin();
					continue;
				}
			}

			int_hashset::iterator itr = includedIds.find(it->first);
			if (itr == includedIds.end()) { // havent added this random result yet
				Vres.push_back(std::make_pair(it->first, (double)0));
				includedIds.insert((*it).first);
				++var;
			}
		}
		return Vres;
	*/
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgID(imageId id, unsigned int numres) {
	/*query for images similar to the one that has this id
	numres is the maximum number of results
	 */

	return queryImgData(get_sig_from_cache(id), numres, 0);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgIDFiltered(imageId id, unsigned int numres, bloom_filter* bf) {
	/*query for images similar to the one that has this id
	numres is the maximum number of results
	 */

	return queryImgDataFiltered(get_sig_from_cache(id), numres, 0, bf, false);
}

template<bool is_simple>
sim_vector dbSpaceImpl<is_simple>::queryImgIDFast(imageId id, unsigned int numres) {
	/*query for images similar to the one that has this id
	numres is the maximum number of results
	 */

	return queryImgDataFast(get_sig_from_cache(id), numres, 0);
}

template<bool is_simple>
void dbSpaceImpl<is_simple>::removeImage(imageId id) {
	SigStruct* isig = find(id).sig();
	ImgData nsig;
	read_sig_cache(isig->cacheOfs, &nsig);

	for (int i = 0; 0 && i < NUM_COEFS; i++) {
		FLIP(nsig.sig1[i]); FLIP(nsig.sig2[i]); FLIP(nsig.sig3[i]);
//fprintf(stderr, "\r%d %d %d %d", i, nsig.sig1[i], nsig.sig2[i], nsig.sig3[i]);
		if (nsig.sig1[i]>0) imgbuckets[0][0][nsig.sig1[i]].remove(nsig.id);
		if (nsig.sig1[i]<0) imgbuckets[0][1][-nsig.sig1[i]].remove(nsig.id);

		if (nsig.sig2[i]>0) imgbuckets[1][0][nsig.sig2[i]].remove(nsig.id);
		if (nsig.sig2[i]<0) imgbuckets[1][1][-nsig.sig2[i]].remove(nsig.id);

		if (nsig.sig3[i]>0) imgbuckets[2][0][nsig.sig3[i]].remove(nsig.id);
		if (nsig.sig3[i]<0) imgbuckets[2][1][-nsig.sig3[i]].remove(nsig.id);   
	}

	m_bucketsValid = false;
	m_images.erase(id);
	delete isig;
}

template<bool is_simple>
Score dbSpaceImpl<is_simple>::calcAvglDiff(imageId id1, imageId id2) {
	/* return the average luminance difference */

	// are images on db ?
	const lumin_int& avgl1 = find(id1).avgl();
	const lumin_int& avgl2 = find(id2).avgl();
	return abs(avgl1[0] - avgl2[0]) + abs(avgl1[1] - avgl2[1]) + abs(avgl1[2] - avgl2[2]);
}	

template<bool is_simple>
Score dbSpaceImpl<is_simple>::calcSim(imageId id1, imageId id2, bool ignore_color)
{
	/* use it to tell the content-based difference between two images
	 */
	ImgData dsig1 = get_sig_from_cache(id1);
	ImgData dsig2 = get_sig_from_cache(id2);

	const Idx* sig1[3] = { dsig1.sig1, dsig1.sig2, dsig1.sig3 };
	const Idx* sig2[3] = { dsig2.sig1, dsig2.sig2, dsig2.sig3 };

	int cnum = ignore_color ? 1 : 3;

	Score score = 0, scale = 0;
	const lumin_int& avgl1 = find(id1).avgl();
	const lumin_int& avgl2 = find(id2).avgl();

	for (int c = 0; c < cnum; c++)
		score += (((DScore)weights[0][0][c]) * abs(avgl1[c] - avgl2[c])) >> ScoreScale;

	for (int b = 0; b < NUM_COEFS; b++)
		for (int c = 0; c < cnum; c++)
			for (int b2 = 0; b2 < NUM_COEFS; b2++) {
				Score weight = weights[0][imgBin[abs(sig1[c][b])]][c];
				scale -= weight;
				if (sig2[c][b2] == sig1[c][b])
					score -= weight;
			}

	scale = ((DScore) ScoreMax) * ScoreMax / scale;
	return (((DScore) score) * 100 * scale) >> ScoreScale;
}

template<bool is_simple>
Score dbSpaceImpl<is_simple>::calcDiff(imageId id1, imageId id2, bool ignore_color) {
	return 100 * ScoreMax - calcSim(id1, id2, ignore_color);
}

template<>
void dbSpaceImpl<false>::rehash() {
	for (int c = 0; c < 3; c++)
		for (int p = 0; p < 2; p++)
			for (int i = 0; i < 16384; i++) {
				imageIdIndex_list<false>& list = imgbuckets[c][p][i];
				size_t size = list.size();
				list.clear();
				list.reserve(size & ~pageImgMask);
			}

	for (imageIterator itr = image_begin(); itr != image_end(); ++itr) {
		ImgData dsig;
		read_sig_cache(itr.cOfs(), &dsig);
		addSigToBuckets(&dsig);
	}

	m_bucketsValid = true;
}

template<> void dbSpaceImpl<true>::rehash() { throw usage_error("Invalid for read-only db."); }

template<bool is_simple>
size_t dbSpaceImpl<is_simple>::getImgCount() {
	if (is_simple && m_sigFile == -1)
		return m_nextIndex;
	else
		return m_images.size();
}

template<bool is_simple>
stats_t dbSpaceImpl<is_simple>::getCoeffStats() {
	stats_t ret;
	ret.reserve(sizeof(imgbuckets) / sizeof(imgbuckets[0][0][0]));

	for (int c = 0; c < 3; c++)
	  for (int s = 0; s < 2; s++)
	    for (int i = 0; i < 16384; i++) {
		uint32_t id = (c << 24) | (s << 16) | i;
		ret.push_back(std::make_pair(id, imgbuckets[c][s][i].size()));
	    }

	return ret;
}

/*
bloom_filter* getIdsBloomFilter(const int dbId) {
	return validate_dbid(dbId)->imgIdsFilter;
}
*/

template<bool is_simple>
imageId_list dbSpaceImpl<is_simple>::getImgIdList() {
	imageId_list ids;

	for (imageIterator it = image_begin(); it != image_end(); ++it) {
		ids.push_back(it.id());
	}

	return ids;
}

template<> image_info_list dbSpaceImpl<false>::getImgInfoList() {
	image_info_list info;

	// TODO is there a faster way for getting a maps key list and returning a vector from it ?
	for (imageIterator it = image_begin(); it != image_end(); ++it) {
		info.push_back(image_info(it.id(), it.avgl(), it.width(), it.height()));
	}

	return info;
}
template<> image_info_list dbSpaceImpl<true>::getImgInfoList() { return m_info; }

/*
// return structure containing filter with all image ids that have this keyword
keywordStruct* getKwdPostings(int hash) {
	keywordStruct* nks;
	if (globalKwdsMap.find(hash) == globalKwdsMap.end()) { // never seen this keyword, create new postings list
		nks = new keywordStruct();		
		globalKwdsMap[hash] = nks;		
	} else { // already know about it just fetch then
		nks = globalKwdsMap[hash];
	}
	return nks;
}

// keywords in images
bool addKeywordImg(const int dbId, const int id, const int hash) {		
	validate_imgid(dbId, id);

	// populate keyword postings
	getKwdPostings(hash)->imgIdsFilter->insert(id);

	// populate image kwds
	if (!dbSpace[dbId]->sigs[id]->keywords) dbSpace[dbId]->sigs[id]->keywords = new int_hashset;
	return dbSpace[dbId]->sigs[id]->keywords->insert(hash).second;
}

bool addKeywordsImg(const int dbId, const int id, int_vector hashes){
	validate_imgid(dbId, id);

	// populate keyword postings
	for (intVectorIterator it = hashes.begin(); it != hashes.end(); it++) {			
		getKwdPostings(*it)->imgIdsFilter->insert(id);
	}

	// populate image kwds
	if (!dbSpace[dbId]->sigs[id]->keywords) dbSpace[dbId]->sigs[id]->keywords = new int_hashset;
	int_hashset& imgKwds = *dbSpace[dbId]->sigs[id]->keywords;
	imgKwds.insert(hashes.begin(),hashes.end());
	return true;
}

bool removeKeywordImg(const int dbId, const int id, const int hash){
	validate_imgid(dbId, id);

	//TODO remove from kwd postings, maybe creating an API method for regenerating kwdpostings filters or 
	// calling it internally after a number of kwd removes
	if (!dbSpace[dbId]->sigs[id]->keywords) dbSpace[dbId]->sigs[id]->keywords = new int_hashset;
	return dbSpace[dbId]->sigs[id]->keywords->erase(hash);
}

bool removeAllKeywordImg(const int dbId, const int id){
	validate_imgid(dbId, id);
	//TODO remove from kwd postings
	//dbSpace[dbId]->sigs[id]->keywords.clear();
	delete dbSpace[dbId]->sigs[id]->keywords;
	dbSpace[dbId]->sigs[id]->keywords = NULL;
	return true;
}

std::vector<int> getKeywordsImg(const int dbId, const int id){
	validate_imgid(dbId, id);
	int_vector ret;
	if (!dbSpace[dbId]->sigs[id]->keywords) return ret;
	int_hashset& imgKwds = *dbSpace[dbId]->sigs[id]->keywords;
	ret.insert(ret.end(),imgKwds.begin(),imgKwds.end());
	return ret;
}

// query by keywords
std::vector<int> mostPopularKeywords(const int dbId, std::vector<imageId> imgs, std::vector<int> excludedKwds, int count, int mode) {

	kwdFreqMap freqMap = kwdFreqMap();

	for (longintVectorIterator it = imgs.begin(); it != imgs.end(); it++) {
		if (!dbSpace[dbId]->sigs[*it]->keywords) continue;
		int_hashset& imgKwds = *dbSpace[dbId]->sigs[*it]->keywords;

		for (int_hashset::iterator itkw = imgKwds.begin(); itkw != imgKwds.end(); itkw++) {			
			if (freqMap.find(*itkw) == freqMap.end()) {
				freqMap[*itkw] = 1;
			} else {
				freqMap[*itkw]++;
			}
		}
	}

	kwdFreqPriorityQueue pqKwds;

	int_hashset setExcludedKwds = int_hashset();

	for (intVectorIterator uit = excludedKwds.begin(); uit != excludedKwds.end(); uit++) {
		setExcludedKwds.insert(*uit);
	}
	
	for (kwdFreqMap::iterator it = freqMap.begin(); it != freqMap.end(); it++) {
		int id = it->first;
		long int freq = it->second;
		if (setExcludedKwds.find(id) != setExcludedKwds.end()) continue; // skip excluded kwds
		pqKwds.push(KwdFrequencyStruct(id,freq));
	}

	int_vector res = int_vector();

	while (count >0 && !pqKwds.empty()) {
		KwdFrequencyStruct kf = pqKwds.top();
		pqKwds.pop();
		res.push_back(kf.kwdId);
		res.push_back(kf.freq);
		count--;
	}
	
	return res;

}

// query by keywords
sim_vector queryImgIDKeywords(const int dbId, imageId id, unsigned int numres, int kwJoinType, int_vector keywords){
	validate_dbid(dbId);

	if (id != 0) validate_imgid(dbId, id);

	// populate filter
	intVectorIterator it = keywords.begin();
	bloom_filter* bf = 0;
	if (*it == 0) {
		// querying all tags
	} else {
		// OR or AND each kwd postings filter to get final filter
		// start with the first one
		bf = new bloom_filter(*(getKwdPostings(*it)->imgIdsFilter));
		it++;
		for (; it != keywords.end(); it++) { // iterate the rest
			if (kwJoinType) { // and'd
				(*bf) &= *(getKwdPostings(*it)->imgIdsFilter);
			} else { // or'd
				(*bf) |= *(getKwdPostings(*it)->imgIdsFilter);
			}
		}
	}

	if (id == 0) { // random images with these kwds

		sim_vector V; // select all images with the desired keywords		
		for (sigIterator sit = dbSpace[dbId]->sigs.begin(); sit != dbSpace[dbId]->sigs.end(); sit++) {
			if (V.size() > 20*numres) break;

			if ((bf == 0) || (bf->contains((*sit).first))) { // image has desired keyword or we're querying random
				// XXX V.push_back(std::make_pair(sit->first, (double)1));
			}
		}

		sim_vector Vres;

		for (size_t var = 0; var < std::min<size_t>(V.size(), numres); ) { // var goes from 0 to numres
			int rint = rand()%V.size();
			if (V[rint].second > 0) { // havent added this random result yet
				// XXX Vres.push_back(std::make_pair(V[rint].first, (double)0));
				V[rint].second = 0;
				++var;
			}
			++var;
		}

		return Vres;
	}
	return queryImgIDFiltered(dbId, id, numres, bf);

}
sim_vector queryImgIDFastKeywords(const int dbId, imageId id, unsigned int numres, int kwJoinType, int_vector keywords){
	validate_imgid(dbId, id);

	throw usage_error("not yet implemented");
}
sim_vector queryImgDataFastKeywords(const int dbId, int * sig1, int * sig2, int * sig3, Score *avgl, unsigned int numres, int flags, int kwJoinType, std::vector<int> keywords){
	validate_dbid(dbId);

	throw usage_error("not yet implemented");
}
std::vector<imageId> getAllImgsByKeywords(const int dbId, const unsigned int numres, int kwJoinType, std::vector<int> keywords){
	validate_dbid(dbId);

	std::vector<imageId> res; // holds result of img lists

	if (keywords.empty())
		throw usage_error("ERROR: keywords list must have at least one hash");

	// populate filter
	intVectorIterator it = keywords.begin();

	// OR or AND each kwd postings filter to get final filter
	// start with the first one
	bloom_filter* bf = new bloom_filter(*(getKwdPostings(*it)->imgIdsFilter));
	it++;
	for (; it != keywords.end(); it++) { // iterate the rest
		if (kwJoinType) { // and'd
			(*bf) &= *(getKwdPostings(*it)->imgIdsFilter);
		} else { // or'd
			(*bf) |= *(getKwdPostings(*it)->imgIdsFilter);
		}
	}

	for (sigIterator sit = dbSpace[dbId]->sigs.begin(); sit != dbSpace[dbId]->sigs.end(); sit++) {
		if (bf->contains((*sit).first)) res.push_back((*sit).first);
		if (res.size() >= numres) break; // ok, got enough
	}
	delete bf;
	return res;
}
double getKeywordsVisualDistance(const int dbId, int distanceType, std::vector<int> keywords){
	validate_dbid(dbId);

	throw usage_error("not yet implemented");
}

// keywords
std::vector<int> getKeywordsPopular(const int dbId, const unsigned int numres) {
	validate_dbid(dbId);

	throw usage_error("not yet implemented");
}

// clustering

std::vector<clustersStruct> getClusterDb(const int dbId, const int numClusters) {
	validate_dbid(dbId);
	throw usage_error("not yet implemented");
} 
std::vector<clustersStruct> getClusterKeywords(const int dbId, const int numClusters,std::vector<int> keywords) {
	validate_dbid(dbId);
	throw usage_error("not yet implemented");
}
*/

dbSpace::dbSpace() { }
dbSpace::~dbSpace() { }

template<bool is_simple>
dbSpaceImpl<is_simple>::dbSpaceImpl(bool with_struct) :
	m_sigFile(-1),
	m_cacheOfs(0),
	m_nextIndex(0),
	m_bucketsValid(true) {

	if (!imgBinInited) initImgBin();

	pageSize = sysconf(_SC_PAGESIZE);
	pageMask = pageSize - 1;
	pageImgs = pageSize / sizeof(imageId);
	pageImgMask = pageMask / sizeof(imageId);
//fprintf(stderr, "page size = %zx, page mask = %zx.\n", pageSize, pageMask);

	if (with_struct)
		m_sigFile = tempfile();

	// imgIdsFilter = new bloom_filter(AVG_IMGS_PER_DBSPACE, 1.0/(100.0 * AVG_IMGS_PER_DBSPACE),random_bloom_seed);
}

template<>
dbSpaceImpl<false>::~dbSpaceImpl() {
	close(m_sigFile);
	// delete imgIdsFilter;
	for (imageIterator itr = image_begin(); itr != image_end(); ++itr)
		delete itr.sig();
}

template<>
dbSpaceImpl<true>::~dbSpaceImpl() {
	if (m_sigFile != -1) close(m_sigFile);
	// delete imgIdsFilter;
}

template<bool is_simple>
size_t dbSpaceImpl<is_simple>::get_sig_cache() {
	size_t ofs = m_cacheOfs;
	m_cacheOfs += sizeof(ImgData);
	return ofs;
}

template<bool is_simple>
void dbSpaceImpl<is_simple>::read_sig_cache(size_t ofs, ImgData* sig) {
	if (m_sigFile == -1) throw internal_error("Can't read sig cache when using simple db.");
	if (lseek(m_sigFile, ofs, SEEK_SET) == -1) throw io_error("Can't seek in sig cache.");
	if (read(m_sigFile, sig, sizeof(ImgData)) != sizeof(ImgData)) throw io_error("Can't read sig cache.");
}

template<bool is_simple>
void dbSpaceImpl<is_simple>::write_sig_cache(size_t ofs, ImgData* sig) {
	if (m_sigFile == -1) throw internal_error("Can't write sig cache when using simple db.");
	if (lseek(m_sigFile, ofs, SEEK_SET) == -1) throw io_error("Can't seek in sig cache.");
	if (write(m_sigFile, sig, sizeof(ImgData)) != sizeof(ImgData)) throw io_error("Can't write to sig cache.");
}

} // namespace

