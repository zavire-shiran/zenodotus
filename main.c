#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <dirent.h>
#include <getopt.h>
#include <libgen.h>
#include <limits.h>
#include <sha2.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

const char tags_db_filename[] = "tags.sqlite3";
const char hashes_directoryname[] = "hashes";

const char check_table_query[] = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
const char get_version_number_query[] = "SELECT value FROM settings WHERE name='version' LIMIT 1;";

const char* create_tables_queries[] =
{"CREATE TABLE settings (name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);",
 "CREATE TABLE hashes (hash TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);",
 "CREATE TABLE tags (hash TEXT NOT NULL, name TEXT NOT NULL, value TEXT);",
 "INSERT INTO settings VALUES (\"version\", 1);",
 NULL};

const char insert_file_query[] = "INSERT INTO hashes VALUES (?, ?);";
const char check_for_duplicate_query[] = "SELECT * from hashes WHERE hash = ? OR name = ?;";
const char add_tag_query[] = "INSERT INTO tags VALUES (?, ?, ?);";
const char get_hash_by_name_query[] = "SELECT hash FROM hashes WHERE name = ?;";
const char get_hash_by_prefix_query[] = "SELECT hash FROM hashes WHERE instr(hash, ?) == 1;";
const char dump_hashes_by_prefix_query[] = "SELECT hash, name FROM hashes WHERE instr(hash, ?) == 1;";
const char dump_tags_by_hash_query[] = "SELECT name, value FROM tags WHERE hash == ?;";

char* database_file_name = NULL;
size_t database_file_name_length = 0;

typedef int (*subcommand_func)(sqlite3*, int, char**);

typedef struct _subcommand_info {
	const char* name;
	subcommand_func function;
} subcommand_info;

static struct option global_command_line_options[] = {
	{"file",        required_argument,        NULL,        'f'},
	{NULL,          0,                        NULL,         0}
};

static struct option add_command_line_options[] = {
	{NULL,          0,                        NULL,         0}
};

static struct option null_command_line_options[] = {
	{NULL,          0,                        NULL,         0}
};

int isdirempty(const char* dirname) {
	DIR* dir = NULL;
	struct dirent* entry = NULL;
	int ret_code = 0;

	dir = opendir(dirname);
	if(dir == NULL) {
		perror("Error checking isdirempty");
		return 0;
	}

	entry = readdir(dir);
	ret_code = entry != NULL;

	closedir(dir);

	return ret_code;
}

int check_for_setting_table(sqlite3* db) {
	sqlite3_stmt* stmt = NULL;
	int statementdone = 0;
	int found = 0;

	if(sqlite3_prepare_v2(db, check_table_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error checking version: %s\n", sqlite3_errmsg(db));
		return 2;
	}

	while(!statementdone) {
		switch(sqlite3_step(stmt)) {
		case SQLITE_ROW:
			{
				const unsigned char* tablename = sqlite3_column_text(stmt, 0);
				if(strcmp(tablename, "settings") == 0) {
					found = 1;
					statementdone = 1;
				}
			}
			break;
		case SQLITE_DONE:
			statementdone = 1;
			break;
		default:
		// TODO: break this up into separate error handling for different conditions
			fprintf(stderr, "Error checking version: %s\n", sqlite3_errmsg(db));
			statementdone = 1;
			break;
		};
	}

	sqlite3_finalize(stmt);
	if(found) {
		return 0;
	} else {
		return 1;
	}
}

int get_version(sqlite3* db) {
	sqlite3_stmt* stmt = NULL;
	if(sqlite3_prepare_v2(db, get_version_number_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error getting version: %s\n", sqlite3_errmsg(db));
		return -1;
	}

	if(sqlite3_step(stmt) != SQLITE_ROW) {
		fprintf(stderr, "Error getting version: %s\n", sqlite3_errmsg(db));
		return -1;
	}

	int version = sqlite3_column_int(stmt, 0);
	sqlite3_finalize(stmt);
	return version;
}

int create_tables(sqlite3* db) {
	sqlite3_stmt* stmt = NULL;

	for(int i = 0; create_tables_queries[i] != NULL; ++i) {
		const char* query = create_tables_queries[i];

		int rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
		if(rc) {
			fprintf(stderr, "Error creating table: %s\n  query: %s\n", sqlite3_errmsg(db), query);
			sqlite3_finalize(stmt);
			return 1;
		}

		rc = sqlite3_step(stmt);
		if(rc != SQLITE_DONE) {
			fprintf(stderr, "Error creating table: %s\n  query: %s\n", sqlite3_errmsg(db), query);
			sqlite3_finalize(stmt);
			return 1;
		}

		sqlite3_finalize(stmt);
		stmt = NULL;
	}

	return 0;
}

sqlite3* open_database(const char* file_name) {
	sqlite3* db;
	int version = 0;
	int rc;

	rc = sqlite3_open(file_name, &db);
	if(rc) {
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);
		return NULL;
	}

	rc = check_for_setting_table(db);
	// rc == 1 means the table was not found. we'll create it after checking for other errors.
	if(rc == 0) {
		version = get_version(db);
	} else if(rc == 2) {
		//An error message has already been printed.
		return NULL;
	}

	if(version == 0) {
		rc = create_tables(db);
		if(rc > 0) {
			//An error message has already been printed.
			return NULL;
		}
	}

	return db;
}

void close_database(sqlite3* db) {
	sqlite3_close(db);
}

int set_default_filename() {
	char* homedir = getenv("HOME");
	char* default_filename = "/.zenodotus.sqlite3";

	if(homedir == NULL) {
		fprintf(stderr, "ERROR: $HOME is not defined. How does that even happen?\n");
		return 1;
	}

	database_file_name_length = strlen(homedir) + strlen(default_filename) + 1;
	database_file_name = calloc(database_file_name_length, sizeof(char));
	strlcpy(database_file_name, homedir, database_file_name_length);
	strlcat(database_file_name, default_filename, database_file_name_length);

	return 0;
}

int check_for_duplicate(sqlite3* db, const char* name, const char* digest) {
	sqlite3_stmt* stmt = NULL;
	int step_return = 0;

	if(sqlite3_prepare_v2(db, check_for_duplicate_query, -1, &stmt, NULL)) {
		fprintf(stderr, "1 Error checking duplicate: %s\n", sqlite3_errmsg(db));
		return 1;
	}

	if(sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT) ||
	   sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "2 Error checking duplicate: %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	step_return = sqlite3_step(stmt);
	if(step_return != SQLITE_DONE) {
		printf("Duplicate detected:\n");
		do {
			if(step_return == SQLITE_ROW) {
				printf("  %s  %s\n", sqlite3_column_text(stmt, 0), sqlite3_column_text(stmt, 1));
				step_return = sqlite3_step(stmt);
			} else {
				fprintf(stderr, "3 Error checking duplicates: %s\n", sqlite3_errmsg(db));
			}
		} while(step_return == SQLITE_ROW);
		return 1;
	}

	return 0;
}

int add_file(sqlite3* db, char* filename, char* name) {
	char digest[SHA256_DIGEST_STRING_LENGTH];

	if(SHA256File(filename, digest) == NULL) {
		fprintf(stderr, "Error reading file: %s.\n", filename);
		return 1;
	} else {
		pid_t pid;
		sqlite3_stmt* stmt = NULL;

		printf("%s  %s\n", digest, name);

		if(check_for_duplicate(db, name, digest)) {
			return 1;
		}

		if(sqlite3_prepare_v2(db, insert_file_query, -1, &stmt, NULL)) {
			fprintf(stderr, "Error adding file to database: %s\n", sqlite3_errmsg(db));
			return 1;
		}

		if(sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT) ||
		    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT)) {
			fprintf(stderr, "Error adding file to database: %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(stmt);
			return 1;
		}

		if(sqlite3_step(stmt) != SQLITE_DONE) {
			fprintf(stderr, "Error inserting hash for file %s: %s\n", filename, sqlite3_errmsg(db));
			sqlite3_finalize(stmt);
			return 1;
		}

		sqlite3_finalize(stmt);

		if((pid = fork())) {
			int status;
			// Maybe we should make sure of the status?
			if(pid != wait(&status)) {
				fprintf(stderr, "We had more children than we thought!\n");
				return 1;
			} else if(WIFEXITED(status)) {
				return WEXITSTATUS(status);
			} else {
				return 1;
			}
		} else {
			char* argv[4] = {"/bin/mv",filename, NULL, NULL};
			int arglen = strlen(digest) + strlen("hashes/") + 1;
			argv[2] = calloc(arglen, sizeof(char));
			strlcpy(argv[2], "hashes/", arglen);
			strlcat(argv[2], digest, arglen);

			printf("%s \"%s\" \"%s\"\n", argv[0], argv[1], argv[2]);
			execv("/bin/mv", argv);
		}
	}

	return 0;
}

int tag_hash(sqlite3* db, const char* hash, const char* tag, const char* value) {
	sqlite3_stmt* stmt = NULL;
	if(sqlite3_prepare_v2(db, add_tag_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error 1 adding tag to database: %s\n", sqlite3_errmsg(db));
		return 1;
	}

	if(sqlite3_bind_text(stmt, 1, hash, -1, SQLITE_TRANSIENT) ||
	    sqlite3_bind_text(stmt, 2, tag, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "Error 2 adding tag to database: %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	if(value) {
		if(sqlite3_bind_text(stmt, 3, value, -1, SQLITE_TRANSIENT)) {
			fprintf(stderr, "Error 3 adding tag to database: %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(stmt);
			return 1;
		}
	}

	if(sqlite3_step(stmt) != SQLITE_DONE) {
		fprintf(stderr, "Error 4 inserting tag %s for hash %s: %s\n", tag, hash, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	return 0;
}

const char* get_hash_by_prefix(sqlite3* db, const char* hashprefix) {
	sqlite3_stmt* stmt = NULL;
	const char* hash = NULL;
	char* ret_hash = NULL;
	int rc = 0;

	if(sqlite3_prepare_v2(db, get_hash_by_prefix_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error 1 adding getting hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		return NULL;
	}

	if(sqlite3_bind_text(stmt, 1, hashprefix, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "Error 2 getting hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return NULL;
	}

	rc = sqlite3_step(stmt);
	if(rc != SQLITE_ROW) {
		if(rc == SQLITE_DONE) {
			fprintf(stderr, "Error 3 getting hash by prefix %s: No hashes with that prefix\n", hashprefix);
		} else {
			fprintf(stderr, "Error 3 code %d getting hash by prefix %s: %s\n", rc, hashprefix, sqlite3_errmsg(db));
		}
		sqlite3_finalize(stmt);
		return NULL;
	}

	hash = sqlite3_column_text(stmt, 0);
	ret_hash = calloc(strlen(hash) + 1, sizeof(char));
	strlcpy(ret_hash, hash, strlen(hash) + 1);

	rc = sqlite3_step(stmt);
	if(rc != SQLITE_DONE) {
		if(rc == SQLITE_ROW) {
			fprintf(stderr, "Error 4 ambiguous hash prefix %s\n", hashprefix);
		} else {
			fprintf(stderr, "Error 5 error getting hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		}
		free(ret_hash);
		ret_hash = NULL;
	}

	sqlite3_finalize(stmt);
	return ret_hash;
}

int tag_hash_prefix(sqlite3* db, const char* hashprefix, const char* tag, const char* value) {
	const char* hash = get_hash_by_prefix(db, hashprefix);
	int ret = 0;

	if(hash == NULL) {
		return 1;
	}

	ret = tag_hash(db, hash, tag, value);
	return ret;
}

int tag_subcommand(sqlite3* db, int argc, char** argv) {
	int ch = 0;
	const char* hashprefix = NULL;
	const char* tag = NULL;
	const char* value = NULL;

	optreset = 1;
	optind = 1;

	while((ch = getopt_long(argc, argv, "", null_command_line_options, NULL)) != -1) {
		switch(ch) {
		default:
			return 1;
		};
	}

	argc -= optind;
	argv += optind;

	if(argc != 2 && argc != 3) {
		fprintf(stderr, "TODO: tag usage message\n");
		return 1;
	}

	hashprefix = argv[0];
	tag = argv[1];
	if(argc == 3) {
		value = argv[2];
	}

	tag_hash_prefix(db, hashprefix, tag, value);

	return 0;
}

int add_subcommand(sqlite3* db, int argc, char** argv) {
	int ch = 0;

	optreset = 1;
	optind = 1;

	while((ch = getopt_long(argc, argv, "", add_command_line_options, NULL)) != -1) {
		switch(ch) {
			return 1;
		};
	}

	argc -= optind;
	argv += optind;

	if(argc <= 0) {
		fprintf(stderr, "No files to add.\n");
		return 1;
	} else if(argc == 1) {
		char* filebasename = basename(argv[0]);
		return add_file(db, argv[0], filebasename);
	} else if(argc == 2) {
		return add_file(db, argv[0], argv[1]);
	} else {
		fprintf(stderr, "Invalid number of arguments to add subcommand.\n");
		return 1;
	}
}

int initialize_vault(const char* dirname) {
	char* db_filename = NULL;
	char* hashes_dirname = NULL;
	sqlite3* db = NULL;
	int db_filename_size = strlen(dirname) + strlen(tags_db_filename) + 2; // One for '/', one for '\0'
	int hashes_dirname_size = strlen(dirname) + strlen(hashes_directoryname) + 2;

	db_filename = calloc(db_filename_size, sizeof(char));
	strlcpy(db_filename, dirname, db_filename_size);
	strlcat(db_filename, "/", db_filename_size);
	strlcat(db_filename, tags_db_filename, db_filename_size);

	hashes_dirname = calloc(hashes_dirname_size, sizeof(char));
	strlcpy(hashes_dirname, dirname, hashes_dirname_size);
	strlcat(hashes_dirname, "/", hashes_dirname_size);
	strlcat(hashes_dirname, hashes_directoryname, hashes_dirname_size);

	db = open_database(db_filename);
	if(db == NULL) {
		fprintf(stderr, "Unable to create database\n");
		goto error;
	}
	close_database(db);

	if(mkdir(hashes_dirname, 0777)) {
		perror("Error creating hashes directory");
		goto error;
	}

	free(db_filename);
	free(hashes_dirname);
	return 0;

error:
	free(db_filename);
	free(hashes_dirname);
	return 1;
}

int init_subcommand(sqlite3* db, int argc, char** argv) {
	int ch = 0;
	char* dirname = ".";
	char* fulldirname = NULL;
	int ret_code = 0;
	struct stat dirstat;

	optreset = 1;
	optind = 1;

	while((ch = getopt_long(argc, argv, "", null_command_line_options, NULL)) != -1) {
		switch(ch) {
		default:
			return 1;
		};
	}

	argc -= optind;
	argv += optind;

	if(argc > 1) {
		fprintf(stderr, "Too many arguments\n.");
		return 1;
	} else if(argc == 1) {
		dirname = argv[0];
	}

	fulldirname = realpath(dirname, NULL);
	if(fulldirname == NULL) {
		fprintf(stderr, "Invalid directory: %s", dirname);
		return 1;
	}

	if(stat(fulldirname, &dirstat) == -1) {
		perror("Stat failed on fulldirname");
		return 1;
	} else if(S_ISDIR(dirstat.st_mode) == 0) {
		fprintf(stderr, "%s is not a directory.\n", fulldirname);
		return 1;
	}

	if(!isdirempty(fulldirname)) {
		fprintf(stderr, "%s is not empty.\n", fulldirname);
		return 1;
	}

	printf("Initializing zenodotus vault in %s\n", fulldirname);

	ret_code = initialize_vault(fulldirname);
	free(fulldirname);

	return ret_code;
}

int dump_hash_tags(sqlite3* db, const char* hash) {
	sqlite3_stmt* stmt = NULL;
	int rc = 0;

	if(sqlite3_prepare_v2(db, dump_tags_by_hash_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error 1 dump hash tags %s: %s\n", hash, sqlite3_errmsg(db));
		return 1;
	}

	if(sqlite3_bind_text(stmt, 1, hash, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "Error 2 dump hash tags %s: %s\n", hash, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if(sqlite3_column_type(stmt, 1) == SQLITE_TEXT && strlen(sqlite3_column_text(stmt, 1)) > 0) {
			printf("  %s  %s\n", sqlite3_column_text(stmt, 0), sqlite3_column_text(stmt, 1));
		} else {
			printf("  %s\n", sqlite3_column_text(stmt, 0));
		}
	}

	if(rc != SQLITE_DONE) {
		fprintf(stderr, "Error 3 dumping hash tags %s: %s\n", hash, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	sqlite3_finalize(stmt);
	return 0;

}

int dump_hash_prefix(sqlite3* db, const char* hashprefix) {
	sqlite3_stmt* stmt = NULL;
	int rc = 0;

	if(sqlite3_prepare_v2(db, dump_hashes_by_prefix_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error 1 dumping hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		return 1;
	}

	if(sqlite3_bind_text(stmt, 1, hashprefix, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "Error 2 dumping hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		printf("%s  %s\n", sqlite3_column_text(stmt, 1), sqlite3_column_text(stmt, 0));
		if(dump_hash_tags(db, sqlite3_column_text(stmt, 0)) != 0) {
			sqlite3_finalize(stmt);
			return 1;
		}
	}

	if(rc != SQLITE_DONE) {
		fprintf(stderr, "Error 3 dumping hash by prefix %s: %s\n", hashprefix, sqlite3_errmsg(db));
		sqlite3_finalize(stmt);
		return 1;
	}

	sqlite3_finalize(stmt);
	return 0;
}

int dump_subcommand(sqlite3* db, int argc, char** argv) {
	int ch = 0;
	char* prefix = NULL;

	while((ch = getopt_long(argc, argv, "", null_command_line_options, NULL)) != -1) {
		switch(ch) {
		default:
			return 1;
		};
	}

	argc -= optind;
	argv += optind;

	if(argc == 0) {
		prefix = "";
	} else if(argc == 1) {
		prefix = argv[0];
	} else {
		fprintf(stderr, "Too many args\n");
		return 1;
	}

	return dump_hash_prefix(db, prefix);
}

subcommand_info valid_subcommands[] =
 {{"add", add_subcommand},
  {"tag", tag_subcommand},
  {"init", init_subcommand},
  {"dump", dump_subcommand},
  {NULL, NULL}};

int main(int argc, char** argv) {
	sqlite3* db = NULL;
	int ch = 0;
	int return_code = 0;
	subcommand_func subcommand = NULL;
	subcommand_info* subcomm_info = NULL;

	if(set_default_filename()) {
		return 1;
	}

	if(argv[1][0] == '-') {
		while((ch = getopt_long(argc, argv, "f:", global_command_line_options, NULL)) != -1) {
			switch(ch) {
			case 'f':
				database_file_name_length = strlen(optarg) + 1;
				database_file_name = reallocarray(database_file_name, database_file_name_length, sizeof(char));
				strlcpy(database_file_name, optarg, database_file_name_length);
				break;
			default:
				return 1;
			};

			// The first non-option argument must be the subcommand name, so stop there.
			if(argv[optind] != NULL && argv[optind][0] != '-') {
				break;
			}
		}
	}

	argc -= optind;
	argv += optind;

	if(argc <= 0) {
		fprintf(stderr, "No action specified.\n");
		return 1;
	}

	for(subcomm_info = valid_subcommands; subcomm_info->name != NULL; ++subcomm_info) {
		if(strcmp(subcomm_info->name, argv[0]) == 0) {
			subcommand = subcomm_info->function;
		}
	}

	if(subcommand == NULL) {
		fprintf(stderr, "\"%s\" is not a valid subcommand.\n", argv[0]);
		return 1;
	}

	if(strcmp(argv[0], "init")) {
		db = open_database(tags_db_filename);
		if(db == NULL) {
			fprintf(stderr, "Current directory is not a vault.\n");
		}
	}

	return_code = subcommand(db, argc, argv);

	close_database(db);

	return return_code;
}
