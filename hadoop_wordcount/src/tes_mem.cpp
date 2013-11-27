#include <err.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <libgen.h>
#include <errno.h>
#include <unistd.h>
#include <map>
#include <string>
#include <iostream>

#include "libmemcached/memcached.h"

using namespace std;

const int32_t EXPIRATION = 600;

class Cmemcached {
private:
	memcached_st *_memc;
	//memcached_return _rc;
public:
	Cmemcached(string server_list) : _memc(0) {
		//string attr_str = "--REMOVE_FAILED_SERVERS --RETRY-TIMEOUT=1";
		_memc = memcached_create(NULL);
		//_memc= memcached(attr_str.c_str(), strlen(attr_str.c_str()));
		memcached_server_st *servers= memcached_servers_parse(server_list.c_str());
		memcached_server_push(_memc, servers);
		memcached_server_list_free(servers);

		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_RETRY_TIMEOUT, 1);

		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT, 1);
		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS, 1);


		cout << "MEMCACHED_BEHAVIOR_RETRY_TIMEOUT:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_RETRY_TIMEOUT) << endl;
		cout << "MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT) << endl;
		cout << "MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS) << endl;
	}
	~Cmemcached() {
		if (_memc != 0) {
			memcached_free(_memc);
		}
	}

	char *get(const char *key, size_t key_length, size_t *value_length, uint32_t *flags, memcached_return_t *error) {
		return memcached_get(_memc, key, key_length, value_length, flags, error);
	}

	memcached_return set(const char *key, size_t key_length, const char *value, size_t value_length, uint32_t flags) {
		return memcached_set(_memc, key, key_length, value, value_length, EXPIRATION, flags);
	}

	void get_server_list(void) {
		memcached_server_st *servers= memcached_server_list(_memc);
		cout <<"server count = " << memcached_server_list_count(servers) << endl;
		cout <<"hostname= " << servers->hostname << endl;
	}
};

void insert_data(Cmemcached &memcached) {
	map<string, string> kv_map;
	kv_map.insert( std::pair<string, string>("key1", "val1") );
	kv_map.insert( std::pair<string, string>("key2", "val1") );
	kv_map.insert( std::pair<string, string>("key3", "val1") );
	kv_map.insert( std::pair<string, string>("key4", "val1") );
	kv_map.insert( std::pair<string, string>("key5", "val1") );
	kv_map.insert( std::pair<string, string>("key6", "val1") );

	memcached_return rc;
	for (map<string, string>::iterator itr = kv_map.begin(); itr != kv_map.end(); ++itr) {
		rc = memcached.set((itr->first).c_str(), strlen((itr->first).c_str()), (itr->second).c_str(), strlen((itr->second).c_str()), 0);
		cout << "rc=" << rc << endl;

		if (rc != 0) {
			memcached.get_server_list();
		}
	}
}

int main(int argc, char *argv[])
{
	const char *sz_servers = "10.1.145.17:11211, 10.1.144.41:11211";
	Cmemcached memcached(sz_servers);
	insert_data(memcached);

	const char *dgst = "key1";
	size_t val_len;
	uint32_t flags;
	memcached_return rc;

	char *value = memcached.get(dgst, strlen(dgst), &val_len, &flags, &rc);
	printf("rc=%d, return value=%s\n", rc, value);
	if (value) {
		free(value);
	}

	exit(0);
}
