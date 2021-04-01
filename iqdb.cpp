/***************************************************************************\
    iqdb.cpp - iqdb server (database maintenance and queries)

    Copyright (C) 2008 piespy@gmail.com

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

#include <math.h>
#include <stdio.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>

#ifdef MEMCHECK
#include <malloc.h>
#include <mcheck.h>
#endif

#include "auto_clean.h"
#include "imgdb.h"

static void die(const char fmt[], ...) __attribute__ ((format (printf, 1, 2))) __attribute__ ((noreturn));
static void die(const char fmt[], ...) {
	va_list args;

	va_start(args, fmt);
	fflush(stdout);
	vfprintf(stderr, fmt, args);
	va_end(args);

	exit(1);
}

imgdb::dbSpace* loaddb(const char* fn, int mode) {
	imgdb::dbSpace* db = imgdb::dbSpace::load_file(fn, mode);
	fprintf(stderr, "Database loaded from %s, has %zd images.\n", fn, db->getImgCount());
	return db;
}

class dbSpaceAuto : public AutoCleanPtr<imgdb::dbSpace> {
public:
	dbSpaceAuto() { };
	dbSpaceAuto(const char* filename, int mode) : AutoCleanPtr<imgdb::dbSpace>(loaddb(filename, mode)) { };
};

typedef dbSpaceAuto dbSpaceAutoList[];

inline dbSpaceAuto& check(dbSpaceAutoList dbs, unsigned int ndbs, unsigned int dbid) {
	if (dbid >= ndbs)
		throw imgdb::param_error("dbId out of range.");
	return dbs[dbid];
}

#define ScD(x) ((double)(x)/imgdb::ScoreMax)
#define DScD(x) ScD(((x)/imgdb::ScoreMax))

// Minimum score to consider an image a relevant match. Starting from the end of the list
// (least similar), average and standard deviation are computed. When the std.dev. exceeds
// the min_stddev value, the minimum score is then avg + stddev * stddev_fract.
imgdb::Score min_sim(const imgdb::sim_vector& sim, imgdb::Score min_stddev, imgdb::Score stddev_frac) {
	imgdb::DScore min_sqd = (imgdb::DScore)min_stddev * min_stddev;
	imgdb::DScore sum = 0;
	imgdb::DScore sqsum = 0;
	int cnt = 0;
	if (sim.size() < 2) return -1;
	for (imgdb::sim_vector::const_iterator itr = sim.end() - 1; itr != sim.begin(); --itr) {
		if (itr->score < 0) continue;

		cnt++;
		sum += itr->score;
		sqsum += (imgdb::DScore)itr->score * itr->score;

		if (cnt < 2) continue;

		imgdb::DScore avg = sum / cnt;
		imgdb::DScore sqd = sqsum - sum * avg;
		//imgdb::Score stddev = lrint(sqrt((double)sqd/cnt));
		//imgdb::Score min_sim = avg + (((imgdb::DScore)stddev_frac * stddev) >> imgdb::ScoreScale);
//fprintf(stderr, "Got %d. sum=%.1f avg=%.1f sqsum=%.1f² stddev=%.1f. Want %.1f of min=%.1f = %.1f\n", 
//cnt, ScD(sum), ScD(avg), ScD(sqrt(sqsum)), ScD(stddev), ScD(stddev_frac), ScD(min_stddev), DScD((((imgdb::DScore)stddev_frac * stddev))));
		if (sqd > min_sqd * cnt) return avg + lrint((double)stddev_frac * sqrt(sqd/cnt) / imgdb::ScoreMax);
	}
	return -1;
}

class dupe_list : private std::vector<imgdb::sim_vector> {
public:
	typedef std::vector<imgdb::sim_vector> base_type;
	using base_type::at;
	using base_type::size;

	typedef std::map<imgdb::imageId,size_t> dupe_map;
	typedef dupe_map::iterator dupe_set;

	dupe_set not_found() { return m_map.end(); }
	dupe_set find(imgdb::imageId id) { return m_map.find(id); }
	dupe_set create_set(imgdb::imageId id) {
		push_back(base_type::value_type());
		return m_map.insert(dupe_map::value_type(id, size() - 1)).first;
	}
	void merge(size_t set, imgdb::sim_vector& from) {
		for (imgdb::sim_vector::iterator itr = from.begin(); itr != from.end(); ++itr)
			m_map[itr->id] = set;

		imgdb::sim_vector& list = at(set);
		list.insert(list.end(), from.begin(), from.end());
		from.clear();
	}
	void print(FILE *f) {
		for (iterator itr = begin(); itr != end(); ++itr) {
			if (itr->empty()) continue;
			for (imgdb::sim_vector::iterator sItr = itr->begin(); sItr != itr->end(); ++sItr)
				fprintf(f, "%s%08lx", sItr == itr->begin() ? "" : " ", sItr->id);
			fprintf(f, "\n");
		}
	}

private:
	dupe_map m_map;
};

struct id_not_below : public std::unary_function<imgdb::sim_value,bool> {
	id_not_below(imgdb::imageId base) : m_base(base) { }
	bool operator() (const imgdb::sim_value& sim) { return sim.id >= m_base; }
	imgdb::imageId m_base;
};

void find_duplicates(const char* fn, int mindev) {
	/* for testing stddev code...
	int scores[] = { 84, 71, 67, 52, 43, 41, 40, 40, 39, 39, 39, 39, 38, 38, 38, 38, 38 };
	imgdb::sim_vector sim;
	for (unsigned int i =0; i < sizeof(scores)/sizeof(scores[0]); i++)
		sim.push_back(imgdb::sim_value(i, scores[i] << imgdb::ScoreScale, 0, 0));
	imgdb::Score m = min_sim(sim, 5 << imgdb::ScoreScale, imgdb::ScoreMax / 2);
fprintf(stderr, "Min score: %.1f\n", ScD(m));
	return;
	*/
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_readonly);

	dupe_list dupes;

	imgdb::imageId_list images = db->getImgIdList();
	for (imgdb::imageId_list::const_iterator itr = images.begin(); itr != images.end(); ++itr) {
		fprintf(stderr, "%3zd%%\r", 100*(itr - images.begin())/images.size());
		imgdb::sim_vector sim = db->queryImg(imgdb::queryArg(db.ptr(), *itr, 16, imgdb::dbSpace::flag_onlyabove | imgdb::dbSpace::flag_nocommon));

		// This is not needed when imgdb::dbSpace::flag_onlyabove starts working:
		sim.erase(std::partition(sim.begin(), sim.end(), id_not_below(*itr)), sim.end());

		imgdb::Score min = min_sim(sim, mindev << imgdb::ScoreScale, imgdb::ScoreMax / 2);
//fprintf(stderr, "%08lx got %4.1f +/- %4.1f: ", *itr, (double)avg/imgdb::ScoreMax, (double)stddev/imgdb::ScoreMax);
//for (size_t i = 1; i < sim.size(); i++) fprintf(stderr, "%08lx:%4.1f ", sim[i].first, (double)sim[i].second/imgdb::ScoreMax);
//fprintf(stderr, "\n");
		if (min < 0) continue;

		dupe_list::dupe_set set = dupes.not_found();
		for (imgdb::sim_vector::iterator sItr = sim.begin(); sItr != sim.end(); ++sItr) {
			if (sItr->score < min) {
				sim.erase(sItr, sim.end());
				break;
			}

			dupe_list::dupe_set itr = dupes.find(sItr->id);
			if (itr == dupes.not_found()) continue;
			if (set == dupes.not_found()) {
//fprintf(stderr," ID %08lx joining set %08lx:%zd.\n", sItr->id, itr->first, itr->second);
				set = itr;
				continue;
			}
			if (itr->second == set->second) continue;

			// Two IDs from this dupe set are already in two different sets. Merge second set to first.
//fprintf(stderr, "ID %08lx already in set %08lx:%zd and also have %08lx:%zd. Merging. ", sItr->id, itr->first, itr->second, set->first, set->second);
			dupes.merge(set->second, dupes.at(itr->second));
//fprintf(stderr, "Now %zd has %zd, and %zd has %zd.\n", set->second, dupes.at(set->second).size(), itr->second, dupes.at(itr->second).size());
		}

		if (set == dupes.not_found()) {
			set = dupes.create_set(sim[0].id);
//fprintf(stderr, "ID %08lx creating new set %08lx:%zd.\n", sim[0].id, set->first, set->second);
		}

		dupes.merge(set->second, sim);
//		fprintf(stderr, "Set %zd now: ", set->second);
//		for (imgdb::sim_vector::const_iterator itr = dupes.at(set->second).begin(); itr != dupes.at(set->second).end(); ++itr)
//			fprintf(stderr, " %08lx:%.1f", itr->id, ScD(itr->score));
//		fprintf(stderr, "\n");
	}

	dupes.print(stdout);
}

void add(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_alter);
	while (!feof(stdin)) {
		char fn[1024];
		char line[1024];
		imgdb::imageId id;
		int width = -1, height = -1;
		if (!fgets(line, sizeof(line), stdin)) {
			fprintf(stderr, "Read error.\n");
			continue;
		}
		if (sscanf(line, "%lx %d %d:%1023[^\r\n]\n", &id, &width, &height, fn) != 4  &&
		    sscanf(line, "%lx:%1023[^\r\n]\n", &id, fn) != 2) {
			fprintf(stderr, "Invalid line %s\n", line);
			continue;
		}
		try {
			if (!db->hasImage(id)) {
				fprintf(stderr, "Adding %s = %08lx...\r", fn, id);
				db->addImage(id, fn);
			}
			if (width != -1 && height != -1)
				db->setImageRes(id, width, height);
		} catch (const imgdb::simple_error& err) {
                	fprintf(stderr, "%s: %s %s\n", fn, err.type(), err.what());
		}
	}
	db->save_file(fn);
}

void list(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_simple);
	imgdb::imageId_list list = db->getImgIdList();
	for (imgdb::imageId_list::iterator itr = list.begin(); itr != list.end(); ++itr) printf("%08lx\n", *itr);
}

void rehash(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_normal);
	db->rehash();
	db->save_file(fn);
}

void stats(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_simple);
	size_t count = db->getImgCount();
	imgdb::stats_t stats = db->getCoeffStats();
	for (imgdb::stats_t::const_iterator itr = stats.begin(); itr != stats.end(); ++itr) {
		printf("c=%d\ts=%d\ti=%d\t%zd = %zd\n", itr->first >> 24, (itr->first >> 16) & 0xff, itr->first & 0xffff, itr->second, 100 * itr->second / count);
	}
}

void count(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_simple);
	printf("%zd images\n", db->getImgCount());
}

void query(const char* fn, const char* img, int numres, int flags) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_simple);
	imgdb::sim_vector sim = db->queryImg(imgdb::queryArg(img, numres, flags));
	for (size_t i = 0; i < sim.size(); i++)
		printf("%08lx %lf %d %d\n", sim[i].id, (double)sim[i].score / imgdb::ScoreMax, sim[i].width, sim[i].height);
}

void diff(const char* fn, imgdb::imageId id1, imgdb::imageId id2) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_readonly);
	double diff = db->calcDiff(id1, id2);
	printf("%08lx %08lx %lf\n", id1, id2, diff);
}

void sim(const char* fn, imgdb::imageId id, int numres) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_readonly);
	imgdb::sim_vector sim = db->queryImg(imgdb::queryArg(db.ptr(), id, numres, 0));
	for (size_t i = 0; i < sim.size(); i++)
		printf("%08lx %lf %d %d\n", sim[i].id, (double)sim[i].score / imgdb::ScoreMax, sim[i].width, sim[i].height);
}

enum event_t { DO_QUITANDSAVE };

#define DB check(dbs, ndbs, dbid)

typedef std::pair<imgdb::sim_value,int> sim_db_value;
struct cmp_sim_high : public std::binary_function<sim_db_value,sim_db_value,bool> {
	bool operator() (const sim_db_value& one, const sim_db_value& two) { return two.first.score < one.first.score; }
};
struct query_t { unsigned int dbid, numres, flags; };

void do_commands(FILE* rd, FILE* wr, dbSpaceAutoList dbs, int ndbs) {
	imgdb::queryOpt queryOpt;

	while (!feof(rd)) try {
		fprintf(wr, "000 iqdb ready\n");
		fflush(wr);

		char command[1024];
		if (!fgets(command, sizeof(command), rd)) {
			if (feof(rd)) {
				fprintf(wr, "100 EOF detected.\n");
				return;
			} else if (ferror(rd)) {
				fprintf(wr, "300 File error %s\n", strerror(errno));
			} else {
				fprintf(wr, "300 Unknown file error.\n");
			}
			continue;
		}
		//fprintf(stderr, "Command: %s", command);
		char *arg = strchr(command, ' ');
		if (!arg) {
			fprintf(wr, "300 Invalid command: %s\n", command);
			continue;
		}

		*arg++ = 0;
		//fprintf(wr, "100 Command: %s. Arg: %s", command, arg);
		//fflush(wr);

		#ifdef MEMCHECK
		struct mallinfo mi1 = mallinfo();
		#endif
		#if MEMCHECK>1
		mtrace();
		#endif

		if (!strcmp(command, "quit")) {
			fprintf(wr, "100 Done.\n");
			fflush(wr);
			throw DO_QUITANDSAVE;

		} else if (!strcmp(command, "done")) {
			return;

		} else if (!strcmp(command, "list")) {
			int dbid;
			if (sscanf(arg, "%i\n", &dbid) != 1) throw imgdb::param_error("Format: list <dbid>");
			imgdb::imageId_list list = DB->getImgIdList();
			for (size_t i = 0; i < list.size(); i++) fprintf(wr, "100 %08lx\n", list[i]);

		} else if (!strcmp(command, "count")) {
			int dbid;
			if (sscanf(arg, "%i\n", &dbid) != 1) throw imgdb::param_error("Format: count <dbid>");
			fprintf(wr, "101 count=%zd\n", DB->getImgCount());

		} else if (!strcmp(command, "query_opt")) {
			char *opt_arg = strchr(arg, ' ');
			if (!opt_arg) throw imgdb::param_error("Format: query_opt <option> <arguments...>");
			*opt_arg++ = 0;
			if (!strcmp(arg, "mask")) {
				int mask_and, mask_xor;
				if (sscanf(opt_arg, "%i %i\n", &mask_and, &mask_xor) != 2) throw imgdb::param_error("Format: query_opt mask AND XOR");
				queryOpt.mask(mask_and, mask_xor);
				fprintf(wr, "100 Using mask and=%d xor=%d\n", mask_and, mask_xor);
			} else {
				throw imgdb::param_error("Unknown query option");
			}

		} else if (!strcmp(command, "query")) {
			char filename[1024];
			int dbid, flags, numres;
			if (sscanf(arg, "%i %i %i %1023[^\r\n]\n", &dbid, &flags, &numres, filename) != 4)
				throw imgdb::param_error("Format: query <dbid> <flags> <numres> <filename>");

			imgdb::sim_vector sim = DB->queryImg(imgdb::queryArg(filename, numres, flags).coalesce(queryOpt));
			for (size_t i = 0; i < sim.size(); i++)
				fprintf(wr, "200 %08lx %lf %d %d\n", sim[i].id, (double)sim[i].score / imgdb::ScoreMax, sim[i].width, sim[i].height);

		} else if (!strcmp(command, "multi_query")) {
			int count;
			typedef std::vector<query_t> query_list;
			query_list queries;
			imgdb::queryOpt multiOpt = queryOpt;

			do {
				query_t query;
				if (sscanf(arg, "%i %i %i %n", &query.dbid, &query.flags, &query.numres, &count) != 3)
					throw imgdb::param_error("Format: multi_query <dbid> <flags> <numres> [+ <dbid2> <flags2> <numres2>...] <filename>");

				queries.push_back(query);
				arg += count;
			} while (arg[0] == '+' && ++arg);
			char* eol = strchr(arg, '\n'); if (eol) *eol = 0;

			imgdb::ImgData img;
			imgdb::dbSpace::imgDataFromFile(arg, &img);
			queryOpt.reset();

			std::vector<sim_db_value> sim;
			imgdb::Score merge_min = 100 * imgdb::ScoreMax;
			for (query_list::iterator itr = queries.begin(); itr != queries.end(); ++itr) {
				imgdb::sim_vector dbsim = check(dbs, ndbs, itr->dbid)->queryImg(imgdb::queryArg(img, itr->numres + 1, itr->flags).merge(multiOpt));
				if (dbsim.empty()) continue;

				// Scale it so that DBs with different noise levels are all normalized:
				// Pull score of following hit down to 0%, keeping 100% a fix point, then
				// merge and sort list, and pull up 0% to the minimum noise level again.
				// This assumes that result numres+1 is indeed noise, so numres must not
				// be too small. And if we got fewer images than we requested, the DB
				// doesn't even have that many and hence the noise floor is zero.
				imgdb::Score sim_min = dbsim.back().score;
				if (dbsim.size() < itr->numres + 1)
					sim_min = 0;
				else
					dbsim.pop_back();

				merge_min = std::min(merge_min, sim_min);
				imgdb::DScore slope = sim_min == 100 * imgdb::ScoreMax ? imgdb::ScoreMax : 
					(((imgdb::DScore)100 * imgdb::ScoreMax) << imgdb::ScoreScale) / (100 * imgdb::ScoreMax - sim_min);
				imgdb::DScore offset = - slope * sim_min;

				for (imgdb::sim_vector::iterator sitr = dbsim.begin(); sitr != dbsim.end(); ++sitr) {
					sitr->score = (slope * sitr->score + offset) >> imgdb::ScoreScale;
					sim.push_back(std::make_pair(*sitr, itr->dbid));
				}
			}

			std::sort(sim.begin(), sim.end(), cmp_sim_high());
			imgdb::DScore slope = imgdb::ScoreMax - merge_min / 100;
			for (size_t i = 0; i < sim.size(); i++)
				fprintf(wr, "201 %d %08lx %lf %d %d\n", sim[i].second, sim[i].first.id, (double)((slope * sim[i].first.score >> imgdb::ScoreScale) + merge_min) / imgdb::ScoreMax, sim[i].first.width, sim[i].first.height);

		} else if (!strcmp(command, "add")) {
			char fn[1024];
			imgdb::imageId id;
			int dbid;
			int width = -1, height = -1;
			if (sscanf(arg, "%d %lx %d %d:%1023[^\r\n]\n", &dbid, &id, &width, &height, fn) != 5  &&
			    sscanf(arg, "%d %lx:%1023[^\r\n]\n", &dbid, &id, fn) != 3)
				throw imgdb::param_error("Format: add <dbid> <imgid>[ <width> <height>]:<filename>");

			// Could just catch imgdb::param_error, but this is so common here that handling it explicitly is better.
			if (!DB->hasImage(id)) {
				fprintf(wr, "100 Adding %s = %d:%08lx...\n", fn, dbid, id);
				DB->addImage(id, fn);
			}

			if (width > 0 && height > 0)
				DB->setImageRes(id, width, height);

		} else if (!strcmp(command, "remove")) {
			imgdb::imageId id;
			int dbid;
			if (sscanf(arg, "%d %lx", &dbid, &id) != 2)
				throw imgdb::param_error("Format: remove <dbid> <imgid>");

			fprintf(wr, "100 Removing %d:%08lx...\n", dbid, id);
			DB->removeImage(id);

		} else if (!strcmp(command, "set_res")) {
			imgdb::imageId id;
			int dbid, width, height;
			if (sscanf(arg, "%d %lx %d %d\n", &dbid, &id, &width, &height) != 4)
				throw imgdb::param_error("Format: set_res <dbid> <imgid> <width> <height>");

			fprintf(wr, "100 Setting %d:%08lx = %d:%d...\r", dbid, id, width, height);
			DB->setImageRes(id, width, height);

		} else if (!strcmp(command, "list_info")) {
			int dbid;
			if (sscanf(arg, "%i\n", &dbid) != 1) throw imgdb::param_error("Format: list_info <dbid>");
			imgdb::image_info_list list = DB->getImgInfoList();
			for (imgdb::image_info_list::iterator itr = list.begin(); itr != list.end(); ++itr)
				fprintf(wr, "100 %08lx %d %d\n", itr->id, itr->width, itr->height);

		} else if (!strcmp(command, "rehash")) {
			int dbid;
			if (sscanf(arg, "%d", &dbid) != 1)
				throw imgdb::param_error("Format: rehash <dbid>");

			fprintf(wr, "100 Rehashing %d...\n", dbid);
			DB->rehash();

		} else if (!strcmp(command, "coeff_stats")) {
			int dbid;
			if (sscanf(arg, "%d", &dbid) != 1)
				throw imgdb::param_error("Format: coeff_stats <dbid>");

			fprintf(wr, "100 Retrieving coefficient stats for %d...\n", dbid);
			imgdb::stats_t stats = DB->getCoeffStats();
			for (imgdb::stats_t::iterator itr = stats.begin(); itr != stats.end(); ++itr)
				fprintf(wr, "100 %d %zd\n", itr->first, itr->second);

		} else if (!strcmp(command, "saveas")) {
			char fn[1024];
			int dbid;
			if (sscanf(arg, "%d %1023[^\r\n]\n", &dbid, fn) != 2)
				throw imgdb::param_error("Format: saveas <dbid> <file>");

			fprintf(wr, "100 Saving DB %d to %s...\n", dbid, fn);
			DB->save_file(fn);

		} else if (!strcmp(command, "ping")) {
			fprintf(wr, "100 Pong.\n");

		} else {
			throw imgdb::param_error("Unknown command");
		}

		#if MEMCHECK>1
		muntrace();
		#endif
		#ifdef MEMCHECK
		struct mallinfo mi2 = mallinfo();
		if (mi2.uordblks != mi1.uordblks) {
			FILE* f = fopen("memleak.log", "a");
			fprintf(f, "Command used %d bytes of memory: %s %s", mi2.uordblks - mi1.uordblks, command, arg);
			fclose(f);
		}
		#endif

	} catch (const imgdb::simple_error& err) {
		fprintf(wr, "301 %s %s\n", err.type(), err.what());
		fflush(wr);
	}
}

void command(int numfiles, char** files) {
	dbSpaceAuto dbs[numfiles];
	for (int db = 0; db < numfiles; db++)
		dbs[db].set(loaddb(files[db], imgdb::dbSpace::mode_alter));

	try {
		do_commands(stdin, stdout, dbs, numfiles);

	} catch (const event_t& event) {
		if (event != DO_QUITANDSAVE) return;
		for (int db = 0; db < numfiles; db++)
			dbs[db]->save_file(files[db]);
	}
}

// Attach rd/wr FILE to fd and automatically close when going out of scope.
struct socket_stream {
	socket_stream(int sock) :
	  	socket(sock),
		rd(fdopen(sock, "r")),
		wr(fdopen(sock, "w")) {

	  	if (sock == -1 || !rd || !wr) {
			close();
			throw imgdb::io_error("Cannot fdopen socket.");
		}
	}
	~socket_stream() { close(); }
	void close() {
		if (rd) fclose(rd);
		rd=NULL;
		if (wr) fclose(wr);
		wr=NULL;
		if (socket != -1) ::close(socket);
		socket=-1;
	}

	int socket;
	FILE* rd;
	FILE* wr;
};

void server(const char* hostport, int numfiles, char** files) {
	int port;
	char dummy;
	char host[1024];

        int ret = sscanf(hostport, "%1023[^:]:%i%c", host, &port, &dummy);
	if (ret != 2) { strcpy(host, "localhost"); ret = 1 + sscanf(hostport, "%i%c", &port, &dummy); }
	if (ret != 2) die("Can't parse host/port `%s', got %d.\n", hostport, ret);

	int replace = 0;
	if (numfiles > 0 && !strcmp(files[0], "-r")) {
		replace = 1;
		numfiles--;
		files++;
	}

	struct addrinfo hints;
	struct addrinfo* ai;
	bzero(&hints, sizeof(hints));
	hints.ai_family = PF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	if (int ret = getaddrinfo(host, NULL, &hints, &ai)) die("Can't resolve host: %s\n", gai_strerror(ret));

	struct sockaddr_in bindaddr;
	memcpy(&bindaddr, ai->ai_addr, std::min<size_t>(sizeof(bindaddr), ai->ai_addrlen));
	bindaddr.sin_port = htons(port);
	freeaddrinfo(ai);

	if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) die("Can't ignore SIGPIPE: %s\n", strerror(errno));

	int server_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (server_fd == -1) die("Can't create socket: %s\n", strerror(errno));

	int opt = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) die("Can't set SO_REUSEADDR: %s\n", strerror(errno));
	if (bind(server_fd, (struct sockaddr*) &bindaddr, sizeof(bindaddr)) ||
	    listen(server_fd, 64)) {
		if (!replace) die("Can't bind/listen: %s\n", strerror(errno));
		replace = 2;
		fprintf(stderr, "Socket in use, will replace server later.\n");
	} else {
		fprintf(stderr, "Listening on port %d.\n", port);
	}

	dbSpaceAuto dbs[numfiles];
	for (int db = 0; db < numfiles; db++)
		dbs[db].set(loaddb(files[db], imgdb::dbSpace::mode_simple));

	if (replace == 2) {
		int other_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (other_fd == -1)
			die("Can't create socket: %s.\n", strerror(errno));
		if (connect(other_fd, (struct sockaddr*) &bindaddr, sizeof(bindaddr)))
			die("Can't connect to old server: %s.\n", strerror(errno));

		socket_stream stream(other_fd);
		fprintf(stderr, "Sending quit command.\n");
		fputs("quit now\n", stream.wr); fflush(stream.wr);

		char buf[1024];
		while (fgets(buf, sizeof(buf), stream.rd))
			fprintf(stderr, " --> %s", buf);

		int retry = 0;
		fprintf(stderr, "Binding to %08x:%d... ", ntohl(bindaddr.sin_addr.s_addr), ntohs(bindaddr.sin_port));
		while (bind(server_fd, (struct sockaddr*) &bindaddr, sizeof(bindaddr))) {
			if (retry++ > 60) die("Could not bind: %s.\n", strerror(errno));
			fprintf(stderr, "Can't bind yet: %s.\n", strerror(errno));
			sleep(1);
		}
		fprintf(stderr, "bind ok.\n");
		if (listen(server_fd, 64))
			die("Can't listen: %s.\n", strerror(errno));

		fprintf(stderr, "Listening on port %d.\n", port);
	}

	while (1) {
		struct sockaddr_in client;
		socklen_t len = sizeof(client);
		int fd = accept(server_fd, (struct sockaddr*) &client, &len);
		if (fd == -1) die("accept() failed: %s\n", strerror(errno));
		fprintf(stderr, "Accepted connection from %s:%d\n", inet_ntoa(client.sin_addr), client.sin_port);

		socket_stream stream(fd);

		try {
			do_commands(stream.rd, stream.wr, dbs, numfiles);

		} catch (const event_t& event) {
			if (event == DO_QUITANDSAVE) return;

		// Unhandled imgdb::generic_error means it was fatal or completely unknown.
		} catch (const imgdb::generic_error& err) {
			fprintf(stream.wr, "302 %s %s\n", err.type(), err.what());
			throw;

		} catch (...) {
			fprintf(stream.wr, "300 Caught unhandled exception!\n");
			throw;
		}
	}
}

void help() {
	printf(	"Usage: iqdb add|list|help args...\n"
		"\tadd dbfile - Read images to add in the form ID:filename from stdin.\n"
		"\tlist dbfile - List all images in database.\n"
		"\tquery dbfile imagefile [numres] - Find similar images.\n"
		"\tsim dbfile id [numres] - Find images similar to given ID.\n"
		"\tdiff dbfile id1 id2 - Compute difference between image IDs.\n"
		"\tlisten [host:]port dbfile... - Listen on given host/port.\n"
		"\thelp - Show this help.\n"
	);
	exit(1);
}

int main(int argc, char** argv) {
  try {
//	open_swap();
	if (argc < 2) help();

	const char* filename = argv[2];
	int flags = 0;

	if (!strcasecmp(argv[1], "add")) {
		add(filename);
	} else if (!strcasecmp(argv[1], "list")) {
		list(filename);
	} else if (!strncasecmp(argv[1], "query", 5)) {
		if (argv[1][5] == 'u') flags |= imgdb::dbSpace::flag_uniqueset;

		const char* img = argv[3];
		int numres = argc < 6 ? -1 : strtol(argv[4], NULL, 0);
		if (numres < 1) numres = 16;
		query(filename, img, numres, flags);
	} else if (!strcasecmp(argv[1], "diff")) {
		imgdb::imageId id1 = strtoll(argv[3], NULL, 0);
		imgdb::imageId id2 = strtoll(argv[4], NULL, 0);
		diff(filename, id1, id2);
	} else if (!strcasecmp(argv[1], "sim")) {
		imgdb::imageId id = strtoll(argv[3], NULL, 0);
		int numres = argc < 6 ? -1 : strtol(argv[4], NULL, 0);
		if (numres < 1) numres = 16;
		sim(filename, id, numres);
	} else if (!strcasecmp(argv[1], "rehash")) {
		rehash(filename);
	} else if (!strcasecmp(argv[1], "find_duplicates")) {
		int mindev = strtol(argv[3], NULL, 0);
		if (mindev < 1 || mindev > 99) mindev = 10;
		find_duplicates(filename, mindev);
	} else if (!strcasecmp(argv[1], "command")) {
		command(argc-2, argv+2);
	} else if (!strcasecmp(argv[1], "listen")) {
		server(argv[2], argc-3, argv+3);
	} else if (!strcasecmp(argv[1], "statistics")) {
		stats(filename);
	} else if (!strcasecmp(argv[1], "count")) {
		count(filename);
	} else {
		help();
	}

	//closeDbase();
	//fprintf(stderr, "database closed.\n");

  // Handle this specially because it means we need to fix the DB before restarting :(
  } catch (const imgdb::data_error& err) {
	fprintf(stderr, "Data error: %s.\n", err.what());
	exit(10);

  } catch (const imgdb::generic_error& err) {
	fprintf(stderr, "Caught error %s: %s.\n", err.type(), err.what());
	if (errno) perror("Last system error");
  }

  return 0;
}
