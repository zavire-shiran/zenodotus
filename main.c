#include <getopt.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char check_table_query[] = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
char get_version_number_query[] = "SELECT version_number FROM version LIMIT 1;";

char* create_tables_queries[] =
{"CREATE TABLE version (version_number INT NOT NULL);",
 "CREATE TABLE hashes (hash TEXT NOT NULL PRIMARY KEY, file TEXT NOT NULL);",
 "CREATE TABLE tags (hash TEXT NOT NULL PRIMARY KEY, tagname TEXT NOT NULL, tagval TEXT DEFAULT NULL);",
 "INSERT INTO version VALUES (1);",
 NULL};

char* database_file_name = NULL;
size_t database_file_name_length = 0;

static struct option global_command_line_options[] = {
	{"file",        required_argument,        NULL,        'f'},
	{NULL,          0,                        NULL,         0}
};

static struct option add_command_line_options[] = {
	{"inplace",     no_argument,              NULL,        'i'},
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
	
	for(int i = 0; create_tables_queries[i]; ++i) {
		char* query = create_tables_queries[i];
		
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
	// rc == 1 means the table was not found
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

int main(int argc, char** argv) {
	sqlite3* db;
	int ch;

	if(set_default_filename()) {
		return 1;
	}

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
	}

	argc -= optind;
	argv += optind;

	printf("Database file name: %s\n", database_file_name);

	db = open_database(database_file_name);
	if(db == NULL) {
		return 1;
	}
}
