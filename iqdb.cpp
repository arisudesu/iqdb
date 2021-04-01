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

inline dbSpaceAuto& check(dbSpaceAutoList dbs, int ndbs, int dbid) {
	if (dbid >= ndbs)
		throw imgdb::param_error("dbId out of range.");
	return dbs[dbid];
}

void std_dev(const imgdb::sim_vector& sim, imgdb::Score* average, imgdb::Score* stddev) {
	imgdb::DScore sum = 0;
	imgdb::DScore sqsum = 0;

	for (imgdb::sim_vector::const_iterator itr = sim.begin() + 1; itr != sim.end(); ++itr) {
		if (itr->score < 0) break;
		sum += itr->score;
		sqsum += (imgdb::DScore)itr->score * itr->score;
	}
	int cnt = sim.size() - 1;
	if (cnt < 1) { *average = 0; *stddev = 0; return; }

	*average = sum / cnt;
	*stddev = lrint(sqrt((double)sqsum / cnt - (double)*average * *average));
//fprintf(stderr, "cnt=%d sum=%.1f avg=%.1f sqsum=%.1f stddev=%.1f=%.1f\n", cnt, (double)sum/imgdb::ScoreMax, (double)*average/imgdb::ScoreMax, (double)sqsum/imgdb::ScoreMax, sqrt((double)sqsum / cnt - (double)*average * *average) / imgdb::ScoreMax, (double)*stddev/imgdb::ScoreMax);
}

typedef std::map<imgdb::imageId,double> dupe_set;
/*struct dupe_set_ref {
	dupe_set_ref(dupe_set& s) : m_set(&s) { };
	dupe_set& set() { return *m_set; };
	dupe_set* m_set;
};*/
//typedef std::map<imageid, dupe_set_ref> dupe_list;
struct dupe_list : public std::map<imgdb::imageId, dupe_set*> {
	typedef std::map<imgdb::imageId, dupe_set*> base_type;
	struct accessor : public base_type::iterator {
		accessor(const base_type::iterator& itr) : base_type::iterator(itr) { };
		dupe_set& set() { return *(*this)->second; }
		imgdb::imageId   id()  { return (*this)->first; }
	};
};

void find_duplicates(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_readonly);

	std::vector<dupe_set> dupe_sets;
	dupe_list dupes;

	imgdb::imageId_list images = db->getImgIdList();
	for (imgdb::imageId_list::const_iterator itr = images.begin(); itr != images.end(); ++itr) {
		fprintf(stderr, "%3zd%%\r", 100*(itr - images.begin())/images.size());
		imgdb::sim_vector sim = db->queryImgID(*itr, 16);

		imgdb::Score avg, stddev;
		std_dev(sim, &avg, &stddev);
//fprintf(stderr, "%08lx got %4.1f +/- %4.1f: ", *itr, (double)avg/imgdb::ScoreMax, (double)stddev/imgdb::ScoreMax);
//for (size_t i = 1; i < sim.size(); i++) fprintf(stderr, "%08lx:%4.1f ", sim[i].first, (double)sim[i].second/imgdb::ScoreMax);
//fprintf(stderr, "\n");

		if (stddev < (5 << imgdb::ScoreScale) && sim[1].score < (95 << imgdb::ScoreScale)) continue;
		avg += stddev/2;
		imgdb::sim_vector reduced;
		for (imgdb::sim_vector::const_iterator sItr = sim.begin() + 1; sItr != sim.end() && sItr->score > avg; ++sItr)
			if (sItr->id > *itr) reduced.push_back(*sItr);
		if (reduced.empty()) continue;

		fprintf(stdout, "%08lx", *itr);
		for (imgdb::sim_vector::const_iterator sItr = reduced.begin(); sItr != reduced.end() && sItr->score > avg; ++sItr)
			fprintf(stdout, " %08lx:%.1f", sItr->id, (double)sItr->score / imgdb::ScoreMax);
		fprintf(stdout, "\n");
/*

		dupe_list::accessor dItr = dupes.find(*itr);
		imgdb::sim_vector::const_iterator sItr;
		for (sItr = sim.begin() + 1; dItr == dupes.end() && sItr != sim.end(); ++sItr)
			dItr = dupes.find(sItr->first);

		if (dItr == dupes.end()) {
fprintf(stderr, "Making new set: %zd\n", dupe_sets.size());
			dupe_sets.push_back(dupe_set());
			dItr = dupes.insert(std::make_pair(*itr, &dupe_sets.back())).first;
		} else {
			double img_sim = calcSim(0, *itr, dItr->first, isImageGrayscale(0, *itr));
fprintf(stderr, "Adding to set %p of %08lx. Has sim=%4.1f.\n", &dItr.set(), dItr.id(), img_sim);
			dItr.set()[*itr] = img_sim;
			dupes[*itr] = dItr->second;
		}

		for (sItr = sim.begin() + 1; sItr != sim.end() && sItr->second > avg; ++sItr) {
			dItr.set()[sItr->first] = std::max(sItr->second, dItr.set()[sItr->first]);
			dupes[sItr->first] = dItr->second;
		}
fprintf(stderr, "Set %p of %08lx now has %zd dupes, %zd total: ", &dItr.set(), dItr.id(), dItr.set().size(), dupes.size());
for (dupe_set::iterator i = dItr.set().begin(); i != dItr.set().end(); ++i) fprintf(stderr, "%08lx:%4.1f ", i->first, i->second);
fprintf(stderr, "\n");
*/
	}
}

void add(const char* fn) {
	dbSpaceAuto db(fn, imgdb::dbSpace::mode_normal);
	while (!feof(stdin)) {
		char fn[1024];
		imgdb::imageId id;
		int width = -1, height = -1;
		if (fscanf(stdin, "%lx %d %d:%1023[^\r\n]\n", &id, &width, &height, fn) != 4  &&
		    fscanf(stdin, "%lx:%1023[^\r\n]\n", &id, fn) != 2) {
			fprintf(stderr, "Invalid line, got %08lx %d %d:%s.\n", id, width, height, fn);
			continue;
		}
		if (!db->hasImage(id)) {
			fprintf(stderr, "Adding %s = %08lx...\r", fn, id);
			db->addImage(id, fn);
		}
		if (width != -1 && height != -1 && db->hasImage(id)) {
			db->setImageRes(id, width, height);
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
	imgdb::sim_vector sim = db->queryImgFile(img, numres, flags);
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
	imgdb::sim_vector sim = db->queryImgID(id, numres);
	for (size_t i = 0; i < sim.size(); i++)
		printf("%08lx %lf %d %d\n", sim[i].id, (double)sim[i].score / imgdb::ScoreMax, sim[i].width, sim[i].height);
}

enum event_t { DO_QUITANDSAVE };

#define DB check(dbs, ndbs, dbid)

typedef std::pair<imgdb::sim_value,int> sim_db_value;
struct cmp_sim_high : public std::binary_function<sim_db_value,sim_db_value,bool> {
	bool operator() (const sim_db_value& one, const sim_db_value& two) { return two.first.score < one.first.score; }
};
struct query_t { int dbid, numres, flags; };

void do_commands(FILE* rd, FILE* wr, dbSpaceAutoList dbs, int ndbs) {
	while (!feof(rd)) try {
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
			for (size_t i = 0; i < list.size(); i++) fprintf(wr, "%08lx\n", list[i]);

		} else if (!strcmp(command, "count")) {
			int dbid;
			if (sscanf(arg, "%i\n", &dbid) != 1) throw imgdb::param_error("Format: count <dbid>");
			fprintf(wr, "101 count=%zd\n", DB->getImgCount());

		} else if (!strcmp(command, "query")) {
			char filename[1024];
			int dbid, flags, numres;
			if (sscanf(arg, "%i %i %i %1023[^\r\n]\n", &dbid, &flags, &numres, filename) != 4)
				throw imgdb::param_error("Format: query <dbid> <flags> <numres> <filename>");

			imgdb::sim_vector sim = DB->queryImgFile(filename, numres, flags);
			for (size_t i = 0; i < sim.size(); i++)
				fprintf(wr, "200 %08lx %lf %d %d\n", sim[i].id, (double)sim[i].score / imgdb::ScoreMax, sim[i].width, sim[i].height);

		} else if (!strcmp(command, "multi_query")) {
			int count;
			typedef std::vector<query_t> query_list;
			query_list queries;

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

			std::vector<sim_db_value> sim;
			imgdb::Score merge_min = 100 * imgdb::ScoreMax;
			for (query_list::iterator itr = queries.begin(); itr != queries.end(); ++itr) {
				imgdb::sim_vector dbsim = check(dbs, ndbs, itr->dbid)->queryImgData(img, itr->numres + 1, itr->flags);
				if (dbsim.empty()) continue;

				// Scale it so that DBs with different noise levels are all normalized:
				// Pull score of following hit down to 0%, keeping 100% a fix point, then
				// merge and sort list, and pull up 0% to the minimum noise level again.
				// This assumes that result numres+1 is indeed noise, so numres must not
				// be too small.
				imgdb::Score sim_min = dbsim.back().score;
				merge_min = std::min(merge_min, sim_min);
				imgdb::DScore slope = sim_min == 100 * imgdb::ScoreMax ? imgdb::ScoreMax : 
					(((imgdb::DScore)100 * imgdb::ScoreMax) << imgdb::ScoreScale) / (100 * imgdb::ScoreMax - sim_min);
				imgdb::DScore offset = - slope * sim_min;
				dbsim.pop_back();

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
			if (DB->hasImage(id)) {
				//fprintf(wr, "300 Already in db %d: %08lx:%s\n", dbid, id, fn);
				continue;
			}

			fprintf(wr, "100 Adding %s = %d:%08lx...\r", fn, dbid, id);
			DB->addImage(id, fn);

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

		} else if (!strcmp(command, "rehash")) {
			int dbid;
			if (sscanf(arg, "%d", &dbid) != 1)
				throw imgdb::param_error("Format: rehash <dbid>");

			fprintf(wr, "100 Rehashing %d...\n", dbid);
			DB->rehash();

		} else if (!strcmp(command, "saveas")) {
			char fn[1024];
			int dbid;
			if (sscanf(arg, "%d %[^\r\n]\n", &dbid, fn) != 2)
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

		fflush(wr);

	} catch (const imgdb::simple_error& err) {
		fprintf(wr, "301 %s %s\n", err.type(), err.what());
		fflush(wr);
	}
}

void command(int numfiles, char** files) {
	dbSpaceAuto dbs[numfiles];
	for (int db = 0; db < numfiles; db++)
		dbs[db].set(loaddb(files[db], imgdb::dbSpace::mode_normal));

	try {
		do_commands(stdin, stderr, dbs, numfiles);

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
			if (retry++ > 3) die("Could not bind: %s.\n", strerror(errno));
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
		find_duplicates(filename);
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

  } catch (const imgdb::generic_error& err) {
	fprintf(stderr, "Caught error %s: %s.\n", err.type(), err.what());
	if (errno) perror("Last system error");
  }

  return 0;
}
