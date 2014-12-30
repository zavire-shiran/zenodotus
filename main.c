#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

char check_table_query[] = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
//char get_version_number[] = "SELECT

char *create_tables_queries[] = 
{"CREATE TABLE version (version_number INT NOT NULL);",
 "CREATE TABLE hashes (hash TEXT NOT NULL PRIMARY KEY, file TEXT NOT NULL);",
 "CREATE TABLE tags (hash TEXT NOT NULL PRIMARY KEY, tagname TEXT NOT NULL, tagval TEXT DEFAULT NULL);",
 "INSERT INTO version VALUES (1);",
 NULL};

int check_for_version_table(sqlite3* db) {
	sqlite3_stmt* stmt = NULL;
	int rc = 0;
	int statementdone = 0;
	int found = 0;
	
	rc = sqlite3_prepare_v2(db, check_table_query, -1, &stmt, NULL);
	if(rc) {
		fprintf(stderr, "Error checking version: %s\n", sqlite3_errmsg(db));
		return 2;
	}
	
	while(!statementdone) {
		rc = sqlite3_step(stmt);
		switch(rc) {
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
	
	return 0;
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

int main(int argc, char** argv) {
	sqlite3* db;
	int rc;
	int version = 0;

	rc = sqlite3_open("zenodotus.sqlite3", &db);
	if(rc) {
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		sqlite3_close(db);
		return 1;
	}
	
	rc = check_for_version_table(db);
	// rc == 1 means the table was not found
	if(rc == 0) {
		version = get_version(db);
	} else if(rc == 2) {
		//An error message has already been printed.
		return 1;
	}
	
	if(version == 0) {
		rc = create_tables(db);
		if(rc > 0) {
			//An error message has already been printed.
			return 1;	
		}
	}
}
