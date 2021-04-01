/***************************************************************************\
    imglib.h - iqdb library internal definitions

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

/**************************************************************************\
Implementation notes:

The abstract dbSpace class is implemented as two different dbSpaceImpl
classes depending on a bool template parameter indicating whether the DB is
read-only or not. The "false" version has full functionality but is not
optimized for querying. The "true" version is read-only but has much faster
queries. The read-only version can optionally discard image data not needed
for querying from external image files to further reduce memory usage. This
is called "simple mode".

The advantage of this design is maximum code re-use for the two DB usage
patterns: maintenance and querying. Both implementation classes use
different variables, and specifically different iterators to iterate over
all images. The implementation details of the iterators are of course
different but the majority of the actual code is the same for both types.
\**************************************************************************/

#ifndef IMGDBLIB_H
#define IMGDBLIB_H

#include <list>

#include "auto_clean.h"
#include "haar.h"

namespace imgdb {

// Weights for the Haar coefficients.
// Straight from the referenced paper:
const float weightsf[2][6][3]={
		// For scanned picture (sketch=0):
		//    Y      I      Q       idx total occurs
		{{ 5.00f, 19.21f, 34.37f},  // 0   58.58      1 (`DC' component)
			{ 0.83f,  1.26f,  0.36f},  // 1    2.45      3
			{ 1.01f,  0.44f,  0.45f},  // 2    1.90      5
			{ 0.52f,  0.53f,  0.14f},  // 3    1.19      7
			{ 0.47f,  0.28f,  0.18f},  // 4    0.93      9
			{ 0.30f,  0.14f,  0.27f}}, // 5    0.71      16384-25=16359

			// For handdrawn/painted sketch (sketch=1):
			//    Y      I      Q
			{{ 4.04f, 15.14f, 22.62f},
				{ 0.78f,  0.92f,  0.40f},
				{ 0.46f,  0.53f,  0.63f},
				{ 0.42f,  0.26f,  0.25f},
				{ 0.41f,  0.14f,  0.15f},
				{ 0.32f,  0.07f,  0.38f}}
};

union image_id_index {
	imageId id;
	size_t index;
	image_id_index(imageId i) : id(i) { }
	bool operator==(const image_id_index& other) const { return id == other.id; }
	bool operator!=(const image_id_index& other) const { return id != other.id; }
};

struct imageIdIndex_map {
	struct iterator : public std::iterator<std::forward_iterator_tag, image_id_index> { 
		iterator(image_id_index* p) : m_p(p) { }
		image_id_index*	operator->() const { return m_p; }
		image_id_index&	operator* () const { return *m_p; }
		iterator&	operator++() { ++m_p; return *this; }
		iterator	operator++(int) { return m_p++; }
		bool		operator!=(const iterator& other) { return m_p != other.m_p; }
		image_id_index* m_p;
	};
	typedef const image_id_index* const_iterator;
	imageIdIndex_map() : m_base(NULL), m_img(NULL), m_end(NULL) { }
	imageIdIndex_map(void* base, image_id_index* img, size_t size, size_t l)
		: m_base(base), m_img(img), m_end(img + size), m_length(l) { }
	image_id_index& operator[] (size_t ofs) { return m_img[ofs]; }
	bool operator! () { return m_base; }
	void unmap();
	iterator begin() { return m_img; }
	iterator end() { return m_end; }
	size_t size() { return m_end - m_img; }

	void* m_base;
	image_id_index* m_img;
	image_id_index* m_end;
	size_t m_length;
};

// Automatically unmaps the imageIdIndex_map when it goes out of scope.
typedef AutoClean<imageIdIndex_map, &imageIdIndex_map::unmap> AutoImageIdIndex_map;

// Clean up Image when it goes out of scope.
struct ImagePtr {
	ImagePtr(Image* i) : m_image(i) { }
	void destroy() { DestroyImage(m_image); }
	bool operator! () { return !m_image; }
	Image* operator& () { return m_image; }
	Image* m_image;
};
typedef AutoClean<ImagePtr, &ImagePtr::destroy> AutoImage;

// Same for ExceptionInfo and ImageInfo.
struct AutoExceptionInfo : public ExceptionInfo {
	AutoExceptionInfo() { GetExceptionInfo(this); }
	~AutoExceptionInfo() { DestroyExceptionInfo(this); }
};
struct ImageInfoPtr {
	ImageInfoPtr() : m_info(CloneImageInfo(NULL)) { }
	void destroy() { DestroyImageInfo(m_info); }
	ImageInfo* operator->() { return m_info; }
	ImageInfo* operator& () { return m_info; }
	ImageInfo* m_info;
};
typedef AutoClean<ImageInfoPtr, &ImageInfoPtr::destroy> AutoImageInfo;

typedef std::pair<off_t, size_t> imageIdPage;

template<bool is_simple>
class imageIdIndex_list {
public:
	static const size_t threshold = 128;

	imageIdIndex_list() : m_size(0), m_capacity(0) { }

	imageIdIndex_map map_all(bool writable);
	bool empty() { return !m_size && m_tail.empty(); }
	size_t size() { return m_size + m_tail.size(); }
	size_t paged_capacity() { return m_capacity; }
	size_t paged_size() { return m_size; }
	size_t tail_size() { return m_tail.size(); }
	void reserve(size_t num);
	void loaded(size_t num) { if (num > m_capacity) throw data_error("Loaded too many."); m_size = num; }
	void push_back(image_id_index i) { m_tail.push_back(i); if (m_tail.size() >= threshold) page_out(); }
	void remove(image_id_index i);
	void clear();

	static int fd() { return m_fd; }

protected:
	typedef std::vector<imageIdPage> page_list;
	typedef std::vector<image_id_index> image_list;

	void page_out();

	static int m_fd;

	page_list m_pages;
	size_t m_size;
	size_t m_capacity;
	size_t m_baseofs;
	image_list m_tail;
};

class SigStruct;
class bloom_filter;

/* in memory signature structure */
class SigStruct : public image_info {
public:
	size_t index;		/* index into score array for queries */

	//int_hashset* keywords;

	size_t cacheOfs;

	SigStruct(size_t ofs) : cacheOfs(ofs) { };

	SigStruct() { };

	~SigStruct()
	{
		//delete keywords;
	}

	static void avglf2i(const double avglf[3], lumin_int avgl) {
		for (int c = 0; c < 3; c++)
			avgl[c] = lrint(ScoreMax * avglf[c]);
	};

	void init(ImgData* nsig) {
		id = nsig->id;
		height = nsig->height;
		width = nsig->width;
		avglf2i(nsig->avglf, avgl);
	}
};

/*
class KwdFrequencyStruct {
public:
	int kwdId;
	long int freq;	

	KwdFrequencyStruct(int kwdId, long int freq): kwdId(kwdId),freq(freq) {}

	bool operator< (const KwdFrequencyStruct & right) const {
		return freq > (right.freq);
	}
};
*/

// typedefs
//typedef std::map<const int, imageId> kwdFreqMap;

/* Meta data structure */
struct srzMetaDataStruct {
	unsigned int isValidMetadata;
	unsigned int iskVersion;
	unsigned int bindingLang;
	unsigned int isTrial;
	unsigned int compilePlat;
};

template<bool is_simple>
class dbSpaceImpl;

inline Score get_aspect(int width, int height) { return 0; }

template<bool is_simple> class sigMap;

template<>
class sigMap<false> : public std::map<imageId, SigStruct*> {
public:
	void add_sig(imageId id, SigStruct* sig) { (*this)[id] = sig; }
	void add_index(imageId id, size_t index) { throw usage_error("Only valid in read-only mode."); }
};

template<>
class sigMap<true> : public std::map<imageId, size_t> {
public:
	void add_sig(imageId id, SigStruct* sig) { throw usage_error("Not valid in read-only mode."); }
	void add_index(imageId id, size_t index) { (*this)[id] = index; }
};

template<bool is_simple>
struct index_iterator;

// In normal mode, we have image data in the sigMap, so iterate over that.
template<>
struct index_iterator<false> : public sigMap<false>::iterator {
	typedef sigMap<false>::iterator base_type;
	index_iterator(const base_type& itr, dbSpaceImpl<false>& db) : base_type(itr) { }

	imageId id() const { return (*this)->first; }
	SigStruct* sig() const { return (*this)->second; }
	size_t index() const { return sig()->index; }
	const lumin_int& avgl() const { return sig()->avgl; }
	int width() const { return sig()->width; }
	int height() const { return sig()->height; }
	uint32_t set() const { return sig()->set; }
	size_t cOfs() const { return sig()->cacheOfs; }
	Score asp() const { return get_aspect(sig()->width, sig()->height); }
};

// In simple mode, we have only the image_info data available, so iterate over that.
// In read-only mode, we additionally have the index into the image_info array in a sigMap.
// Using functions that rely on this in simple mode will throw a usage_error.
template<>
struct index_iterator<true> : public image_info_list::iterator {
	typedef image_info_list::iterator base_type;
	index_iterator(const base_type& itr, dbSpaceImpl<true>& db) : base_type(itr), m_db(db) { }
	index_iterator(const sigMap<true>::iterator& itr, dbSpaceImpl<true>& db);	// implemented below

	imageId id() const { return (*this)->id; }
	SigStruct* sig() const { throw usage_error("Not valid in read-only mode."); }
	size_t index() const;	// implemented below
	const lumin_int& avgl() const { return (*this)->avgl; }
	int width() const { return (*this)->width; }
	int height() const { return (*this)->height; }
	uint32_t set() const { return (*this)->set; }
	size_t cOfs() const { return index() * sizeof(ImgData); }
	Score asp() const { return 0; }

	dbSpaceImpl<true>& m_db;
};

// Iterate over a bucket of imageIdIndex values.
template<bool is_simple>
struct id_index_iterator;

// In normal mode, the imageIdIndex_map stores image IDs. Get the index from the dbSpace's linked SigStruct.
template<>
struct id_index_iterator<false> : public imageIdIndex_map::iterator {
	id_index_iterator(const imageIdIndex_map::iterator& itr, dbSpaceImpl<false>& db) : imageIdIndex_map::iterator(itr), m_db(db) { }
	size_t index() const;	// Implemented below.

	dbSpaceImpl<false>& m_db;
};

// In read-only/simple mode, the imageIdIndex_map stores the index directly.
template<>
struct id_index_iterator<true> : public imageIdIndex_map::iterator {
	id_index_iterator(const imageIdIndex_map::iterator& itr, dbSpaceImpl<true>& db) : imageIdIndex_map::iterator(itr) { }
	size_t index() const { return (*this)->index; };
};

// DB space implementation.
template<bool is_simple>
class dbSpaceImpl : public dbSpace {
public:
	dbSpaceImpl(bool with_struct);
	virtual ~dbSpaceImpl();

	virtual void load_stream(std::ifstream& f, srzMetaDataStruct& md);
	virtual void save_stream(std::ofstream& f);
	virtual int save_file(const char* filename);

	// Image queries.
	virtual sim_vector queryImgID(imageId id, unsigned int numres);
	virtual sim_vector queryImgIDFast(imageId id, unsigned int numres);
	virtual sim_vector queryImgIDFiltered(imageId id, unsigned int numres, bloom_filter* bf);
	virtual sim_vector queryImgRandom(unsigned int numres);
	virtual sim_vector queryImgData(const ImgData& img, unsigned int numres, int flags);
	virtual sim_vector queryImgDataFast(const ImgData& img, unsigned int numres, int flags);
	virtual sim_vector queryImgFile(const char* filename, unsigned int numres, int flags);

	// Stats.
	virtual size_t getImgCount();
	virtual stats_t getCoeffStats();
	virtual bool hasImage(imageId id);
	virtual int getImageHeight(imageId id);
	virtual int getImageWidth(imageId id);
	virtual bool isImageGrayscale(imageId id);
	virtual imageId_list getImgIdList();
	virtual image_info_list getImgInfoList();

	// DB maintenance.
	virtual int addImage(imageId id, const char* filename);
	virtual int addImageBlob(imageId id, const void *blob, size_t length);
	virtual void setImageRes(imageId id, int width, int height);

	virtual void removeImage(imageId id);
	virtual void rehash();

	// Similarity.
	virtual Score calcAvglDiff(imageId id1, imageId id2);
	virtual Score calcSim(imageId id1, imageId id2, bool ignore_color = false);
	virtual Score calcDiff(imageId id1, imageId id2, bool ignore_color = false);
	virtual const lumin_int& getImageAvgl(imageId id);

protected:
	typedef index_iterator<is_simple> imageIterator;
	typedef sigMap<is_simple> image_map;
	typedef typename image_map::iterator map_iterator;

	typedef id_index_iterator<is_simple> idIndexIterator;

	friend struct index_iterator<is_simple>;
	friend struct id_index_iterator<is_simple>;

	image_info_list& info() { return m_info; }
	imageIterator find(imageId i);

private:
	void operator = (const dbSpaceImpl&);

	imageIterator image_begin();
	imageIterator image_end();

	void addSigToBuckets(ImgData* nsig);
	int addImageFromImage(imageId id, Image* image);

	size_t get_sig_cache();
	ImgData get_sig_from_cache(imageId i);
	void write_sig_cache(size_t ofs, ImgData* sig);
	void read_sig_cache(size_t ofs, ImgData* sig);

	template<int num_colors>
	sim_vector do_query(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags, bloom_filter* bfilter, bool fast);

	sim_vector queryImgDataFiltered(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags, bloom_filter* bfilter, bool fast);
	sim_vector queryImgDataFiltered(const ImgData& img, unsigned int numres, int flags, bloom_filter* bfilter, bool fast);
	sim_vector queryImgData(const Idx* sig1, const Idx* sig2, const Idx* sig3, const lumin_int& avgl, unsigned int numres, int flags);

	int m_sigFile;
	size_t m_cacheOfs;

	image_map m_images;

	size_t m_nextIndex;
	image_info_list m_info;

	/* Lists of picture ids, indexed by [color-channel][sign][position], i.e.,
	   R=0/G=1/B=2, pos=0/neg=1, (i*NUM_PIXELS+j)
	 */
	imageIdIndex_list<is_simple> imgbuckets[3][2][16384];
	bool m_bucketsValid;
};

/* signature structure */
static const unsigned int AVG_IMGS_PER_DBSPACE = 20000;

// Serialization constants

static const unsigned int	SRZ_VERSIONED			= 1;
static const unsigned int	SRZ_V0_5_1			= 1;
static const unsigned int	SRZ_V0_6_0			= 2;
static const unsigned int	SRZ_V0_6_1			= 3;
static const unsigned int	SRZ_SINGLE_DBSPACE		= 1;
static const unsigned int	SRZ_MULTIPLE_DBSPACE		= 2;
static const unsigned int	SRZ_TRIAL_VERSION		= 1;
static const unsigned int	SRZ_FULL_VERSION		= 2;
static const unsigned int	SRZ_PLAT_WINDOWS		= 1;
static const unsigned int	SRZ_PLAT_LINUX			= 2;
static const unsigned int	SRZ_LANG_PYTHON			= 1;
static const unsigned int	SRZ_LANG_JAVA			= 2;

/* keyword postings structure */
static const unsigned int	 AVG_IMGS_PER_KWD	= 1000;

/*
class keywordStruct {
	//std::vector<imageId> imgIds;	// img list
public:
	keywordStruct() {
		imgIdsFilter = new bloom_filter(AVG_IMGS_PER_KWD, 1.0/(100.0 * AVG_IMGS_PER_KWD),random_bloom_seed);
	}
	bloom_filter* imgIdsFilter;

	~keywordStruct()
	{
		delete imgIdsFilter;
	}
} ;

typedef std::map<const int, keywordStruct*> keywordsMapType;
typedef std::map<const int, keywordStruct*>::iterator  keywordsMapIterator;
*/

// clustering
/* cluster list structure */
struct clustersStruct {
	imageId id;			/* representative image id */
	std::vector<imageId> imgIds;	/* img list */
	double diameter;		
};

typedef std::list<clustersStruct> cluster_list;
typedef cluster_list::iterator cluster_listIterator;

std::vector<clustersStruct> getClusterDb(const int dbId, const int numClusters);
std::vector<clustersStruct> getClusterKeywords(const int dbId, const int numClusters,std::vector<int> keywords);

// summaries
bloom_filter* getIdsBloomFilter(const int dbId);

// util
// keywordStruct* getKwdPostings(int hash);

// Delayed implementations.
index_iterator<true>::index_iterator(const sigMap<true>::iterator& itr, dbSpaceImpl<true>& db)
  : base_type(db.info().begin() + itr->second), m_db(db) { }
size_t index_iterator<true>::index() const { return *this - m_db.info().begin(); }
size_t id_index_iterator<false>::index() const { return m_db.find((*this)->id).index(); }

} // namespace

#endif
