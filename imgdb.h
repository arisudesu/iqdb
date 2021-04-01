/***************************************************************************\
    imgdb.h - iqdb library API

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

#ifndef IMGDBASE_H
#define IMGDBASE_H

// STL includes
#include <map>
#include <stdexcept>
#include <vector>

// Haar transform defines
#include "haar.h"

namespace imgdb {

// Global typedefs and consts.
typedef unsigned long int imageId;
typedef int32_t Score;
typedef int64_t DScore;

// Exceptions.
class generic_error : public std::exception {
public:
	generic_error(const char* what) throw() : m_what(what), m_type("generic_error") { }
	virtual const char* what() const throw() { return m_what; }
	virtual const char* type() const throw() { return m_type; }

protected:
	generic_error(const char* what, const char* type) throw() : m_what(what), m_type(type) { }
private:
	const char* m_what;
	const char* m_type;
};

#define DEFINE_ERROR(derived, base) \
	class derived : public base { \
	public: \
		derived(const char* what) throw() : base(what, #derived) { } \
	protected: \
		derived(const char* what, const char* type) throw() : base(what, type) { } \
	};

// Fatal, cannot recover, should discontinue using the dbSpace throwing it.
DEFINE_ERROR(fatal_error,     generic_error)

DEFINE_ERROR(usage_error,     fatal_error)	// Program was using library incorrectly.
DEFINE_ERROR(io_error,        fatal_error)	// Non-recoverable IO error while read/writing database or cache.
DEFINE_ERROR(data_error,      fatal_error)	// Database has internally inconsistent data.
DEFINE_ERROR(memory_error,    fatal_error)	// Database could not allocate memory.
DEFINE_ERROR(internal_error,  fatal_error)	// The library code has a bug.

// Non-fatal, may retry the call after correcting the problem, and continue using the library.
DEFINE_ERROR(simple_error,    generic_error)

DEFINE_ERROR(param_error,     simple_error)	// An argument was invalid, e.g. non-existent image ID.
DEFINE_ERROR(image_error,     simple_error)	// Could not successfully extract image data from the given file.

const Score ScoreScale = 20;
const Score ScoreMax = (1 << ScoreScale);

typedef signed int lumin_int[3];

struct sim_value {
	sim_value(imageId i, Score s, int w, int h) : id(i), score(s), width(w), height(h) { }
	imageId id;
	Score score;
	int width, height;
};

struct image_info {
	image_info() { }
	image_info(imageId i, const lumin_int& a, int w, int h) : id(i), width(w), height(h) { memcpy(avgl, a, sizeof(avgl)); }
	imageId id;
	lumin_int avgl;
	union {
		struct {
			uint16_t width;
			uint16_t height;
		};
		uint32_t set;
	};
};

typedef std::vector<sim_value> sim_vector;
typedef std::vector<std::pair<uint32_t, size_t> > stats_t;
typedef std::vector<imageId> imageId_list;
typedef std::vector<image_info> image_info_list;
typedef signed int lumin_int[3];

struct ImgData {
	imageId id;			/* picture id */
	Idx sig1[NUM_COEFS];		/* Y positions with largest magnitude */
	Idx sig2[NUM_COEFS];		/* I positions with largest magnitude */
	Idx sig3[NUM_COEFS];		/* Q positions with largest magnitude */
	double avglf[3];		/* YIQ for position [0,0] */
	/* image properties extracted when opened for the first time */
	int width;			/* in pixels */
	int height;			/* in pixels */
};

class dbSpace;
class bloom_filter;
struct srzMetaDataStruct;

class dbSpaceMap : public std::map<int, dbSpace*> {
	static dbSpaceMap  load_file(const char* filename, int  mode);
	int                save_file(const char* filename);
};

class dbSpace {
public:
	static const int mode_normal    = 0x00; // Full functionality, but slower queries.
	static const int mode_readonly	= 0x01; // Fast queries, but no modifications.
	static const int mode_simple	= 0x02; // Fast queries, less memory, no image ID queries.

	// Image query flags.
	static const int flag_sketch	= 0x01;	// Image is a sketch, use adjusted weights.
	static const int flag_grayscale	= 0x02;	// Disregard color information from image.
	static const int flag_onlyabove	= 0x04;	// For dupe finding: only check and return images beyond selected.
	static const int flag_uniqueset	= 0x08;	// Return only best match from each set.
	static const int flag_nocommon	= 0x10;	// Disregard common coefficients (those which are present in at least 10% of the images).

	static dbSpace*    load_file(const char* filename, int mode);
	virtual int        save_file(const char* filename) = 0;

	virtual ~dbSpace();

	// Image queries.
	virtual sim_vector queryImgID(imageId id, unsigned int numres) = 0;
	virtual sim_vector queryImgIDFast(imageId id, unsigned int numres) = 0;
	virtual sim_vector queryImgIDFiltered(imageId id, unsigned int numres, bloom_filter* bf) = 0;
	virtual sim_vector queryImgRandom(unsigned int numres) = 0;
	virtual sim_vector queryImgData(const ImgData& img, unsigned int numres, int flags) = 0;
	virtual sim_vector queryImgDataFast(const ImgData& img, unsigned int numres, int flags) = 0;
	virtual sim_vector queryImgFile(const char* filename, unsigned int numres, int flags) = 0;

	static void imgDataFromFile(const char* filename, ImgData* img);

	// Stats.
	virtual size_t getImgCount() = 0;
	virtual stats_t getCoeffStats() = 0;
	virtual bool hasImage(imageId id) = 0;
	virtual int getImageHeight(imageId id) = 0;
	virtual int getImageWidth(imageId id) = 0;
	virtual bool isImageGrayscale(imageId id) = 0;
	virtual imageId_list getImgIdList() = 0;
	virtual image_info_list getImgInfoList() = 0;

	// DB maintenance.
	virtual int addImage(imageId id, const char* filename) = 0;
	virtual int addImageBlob(imageId id, const void *blob, size_t length) = 0;
	virtual void setImageRes(imageId id, int width, int height) = 0;

	virtual void removeImage(imageId id) = 0;
	virtual void rehash() = 0;

	// Similarity.
	virtual Score calcAvglDiff(imageId id1, imageId id2) = 0;
	virtual Score calcSim(imageId id1, imageId id2, bool ignore_color = false) = 0;
	virtual Score calcDiff(imageId id1, imageId id2, bool ignore_color = false) = 0;
	virtual const lumin_int& getImageAvgl(imageId id) = 0;

protected:
	dbSpace();

	friend class dbSpaceMap;
	virtual void load_stream(std::ifstream& f, srzMetaDataStruct& md) = 0;
	virtual void save_stream(std::ofstream& f) = 0;

private:
	void operator = (const dbSpace&);
};

} // namespace

#endif
