#include <sys/types.h>

#include <getopt.h>
#include <limits.h>
#include <sha2.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char check_table_query[] = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
const char get_version_number_query[] = "SELECT version_number FROM version LIMIT 1;";

const char* create_tables_queries[] =
{"CREATE TABLE version (version_number INT NOT NULL);",
 "CREATE TABLE hashes (hash TEXT NOT NULL PRIMARY KEY, file TEXT NOT NULL);",
 "CREATE TABLE tags (hash TEXT NOT NULL PRIMARY KEY, tagname TEXT NOT NULL, tagval TEXT DEFAULT NULL);",
 "INSERT INTO version VALUES (1);",
 NULL};

const char insert_file_query[] = "INSERT INTO hashes VALUES (?, ?);";
const char check_for_duplicate_query[] = "SELECT * from hashes WHERE hash = ? OR file = ?;";

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
	{"vault",       no_argument,              NULL,        'v'},
	{NULL,          0,                        NULL,         0}
};

int check_for_version_table(sqlite3* db) {
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
				if(strcmp(tablename, "version") == 0) {
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
	
	rc = check_for_version_table(db);
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

int check_for_duplicate(sqlite3* db, const char* filename, const char* digest) {
	sqlite3_stmt* stmt = NULL;
	int step_return = 0;

	if(sqlite3_prepare_v2(db, check_for_duplicate_query, -1, &stmt, NULL)) {
		fprintf(stderr, "Error checking duplicate: %s\n", sqlite3_errmsg(db));
		return 1;
	}

	if(sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT) ||
	   sqlite3_bind_text(stmt, 2, filename, -1, SQLITE_TRANSIENT)) {
		fprintf(stderr, "Error checking duplicate: %s\n", sqlite3_errmsg(db));
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
				fprintf(stderr, "Error checking duplicates: %s\n", sqlite3_errmsg(db));
			}
		} while(step_return == SQLITE_ROW);
		return 1;
	}

	return 0;
}

int add_file(sqlite3* db, const char* filename) {
	char digest[SHA256_DIGEST_STRING_LENGTH];

	if(SHA256File(filename, digest) == NULL) {
		fprintf(stderr, "Error reading file: %s.\n", filename);
		return 1;
	} else {
		char* abspath = realpath(filename, NULL);

		if(abspath != NULL) {
			sqlite3_stmt* stmt = NULL;
			printf("%s  %s\n", digest, abspath);

			if(check_for_duplicate(db, abspath, digest)) {
				return 1;
			}

			if(sqlite3_prepare_v2(db, insert_file_query, -1, &stmt, NULL)) {
				fprintf(stderr, "Error adding file to database: %s\n", sqlite3_errmsg(db));
				return 1;
			}

			if(sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT) ||
			   sqlite3_bind_text(stmt, 2, abspath, -1, SQLITE_TRANSIENT)) {
				fprintf(stderr, "Error adding file to database: %s\n", sqlite3_errmsg(db));
				sqlite3_finalize(stmt);
				return 1;
			}

			if(sqlite3_step(stmt) != SQLITE_DONE) {
				fprintf(stderr, "Error inserting hash for file %s: %s\n", filename, sqlite3_errmsg(db));
				sqlite3_finalize(stmt);
				return 1;
			}

			free(abspath);
			sqlite3_finalize(stmt);
		} else {
			fprintf(stderr, "realpath() failed on %s.\n", filename);
			return 1;
		}
	}

	return 0;
}

int add_subcommand(sqlite3* db, int argc, char** argv) {
	int ch = 0;
	int vault = 0;
	int i;
	int return_code = 0;

	optreset = 1;
	optind = 1;

	while((ch = getopt_long(argc, argv, "v", add_command_line_options, NULL)) != -1) {
		switch(ch) {
		case 'v':
			vault = 1;
			fprintf(stderr, "vault option not implemented.\n");
			return 1;
		default:
			return 1;
		};
	}

	argc -= optind;
	argv += optind;

	if(argc <= 0) {
		fprintf(stderr, "No files to add.\n");
		return 1;
	}

	for(i = 0; argv[i] != NULL; ++i) {
		if(add_file(db, argv[i]) != 0) {
			return_code = 1;
		}
	}

	return return_code;
}

subcommand_info valid_subcommands[] =
 {{"add", add_subcommand},
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

	printf("Database file name: %s\n", database_file_name);

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

	db = open_database(database_file_name);
	if(db == NULL) {
		return 1;
	}

	return_code = subcommand(db, argc, argv);

	close_database(db);

	return return_code;
}
