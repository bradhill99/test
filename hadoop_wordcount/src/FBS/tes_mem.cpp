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
#include "boost/thread.hpp"

using namespace std;

const int32_t EXPIRATION = 600;

class Cmemcached {
private:
	memcached_st *_memc;
	//memcached_return _rc;
	string _server_list;
public:
	Cmemcached(string server_list) : _memc(0), _server_list(server_list) {
		create_connection();
	}
	~Cmemcached() {
		clean();
	}

	void clean() {
		if (_memc != 0) {
			memcached_free(_memc);
		}
	}

	void create_connection() {
		//string attr_str = "--REMOVE_FAILED_SERVERS --RETRY-TIMEOUT=1";
		_memc = memcached_create(NULL);
		//_memc= memcached(attr_str.c_str(), strlen(attr_str.c_str()));
		memcached_server_st *servers= memcached_servers_parse(_server_list.c_str());
		memcached_server_push(_memc, servers);
		memcached_server_list_free(servers);

		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_RETRY_TIMEOUT, 1);

		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT, 1);
		memcached_behavior_set(_memc , MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS, 1);


		cout << "MEMCACHED_BEHAVIOR_RETRY_TIMEOUT:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_RETRY_TIMEOUT) << endl;
		cout << "MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT) << endl;
		cout << "MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS) << endl;
		cout << "MEMCACHED_BEHAVIOR_DISTRIBUTION:" << memcached_behavior_get(_memc , MEMCACHED_BEHAVIOR_DISTRIBUTION) << endl;
	}

	char *get(const char *key, size_t key_length, size_t *value_length, uint32_t *flags, memcached_return_t *error) {
		return memcached_get(_memc, key, key_length, value_length, flags, error);
	}

	memcached_return set(const char *key, size_t key_length, const char *value, size_t value_length, uint32_t flags) {
		return memcached_set(_memc, key, key_length, value, value_length, EXPIRATION, flags);
	}

	void get_server_list(void) {
		clean();
		create_connection();
		memcached_server_st *instance = memcached_server_get_last_disconnect(_memc);
		if (instance != NULL) {
			cout <<"disconnect hostname is:" << instance->hostname << endl;


		}
		memcached_server_st *servers= memcached_server_list(_memc);
		int cnt = memcached_server_list_count(servers);
		cout <<"server count = " << cnt << endl;
		for (int idx = 0;idx < cnt; ++idx) {
			cout <<"hostname= " << servers[idx].hostname << endl;
		}
	}

	// will be invoked by boost thread lib
    void operator()(){
    	while(true) {
    		sleep(10); // execute by 10 seconds
    		get_server_list();
    	}
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

void insert_data(Cmemcached &memcached, const string &key, const string &value) {
	memcached_return rc;
	rc = memcached.set(key.c_str(), strlen(key.c_str()), value.c_str(), strlen(value.c_str()), 0);
	cout << "rc=" << rc << endl;

	if (rc != 0) {
		memcached.get_server_list();
	}
}

int main(int argc, char *argv[])
{
	const char *sz_servers = "10.1.145.17:11211, 10.1.144.41:11211";
	Cmemcached memcached(sz_servers);
	insert_data(memcached);

	if (argc!=2) {
		cout << "test_mem [insert|query]" << endl;

		Cmemcached memcached_monitor(sz_servers);
		boost::thread monitor_thread(boost::ref(memcached_monitor));
		boost::posix_time::time_duration td( 0,0, 1, 0 );
		while(true) {
			if(monitor_thread.timed_join(td)){
				break;
			}
		}
		exit(0);
	}

	cout << "argv[1]=" << argv[1] << endl;
	if (string("insert").compare(argv[1]) == 0 ) {
		// doing insertion
		cout << "insert\n";
		while(1) {
			string key, value;
			cout << "key,value=";
			cin >> key >> value;
			//cout << "key=" << key << ", value=" << value << endl;
			insert_data(memcached, key, value);
			memcached.get_server_list();
		}

	}
	else {
		//doing query
		cout << "query\n";
		while(1) {
			string key;
			size_t val_len;
			uint32_t flags;
			memcached_return rc;

			cout << "key=";
			cin >> key;
			//cout << "key=" << key << ", value=" << value << endl;
			char *value = memcached.get(key.c_str(), strlen(key.c_str()), &val_len, &flags, &rc);
			printf("rc=%d, return value=%s\n", rc, value);
			if (rc != 0) {
				memcached.get_server_list();
			}
			if (value) {
				free(value);
			}
		}
	}


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
