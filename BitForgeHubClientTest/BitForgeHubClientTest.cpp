// BitForgeHubClientTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\BitForgeHubClient\tioclient_async.hpp"

namespace asio = boost::asio;
using std::string;
using std::vector;
using std::map;
using std::shared_ptr;
using std::cout;
using std::vector;
using std::endl;
using tio::async_error_info;
using boost::lexical_cast;

#define ASSERT assert


struct WnpNextHandler
{
	tio::containers::async_list<string>* wnp_list_;

	WnpNextHandler() : wnp_list_(nullptr){}

	void Start(tio::containers::async_list<string>* wnp_list)
	{
		wnp_list_ = wnp_list;
		Beg4Moar();
	}

	void Beg4Moar()
	{
		wnp_list_->wait_and_pop_next(
			[](){},
			[](const async_error_info& error)
			{
				cout << "ERROR: " << endl << endl;
			},
			[this](int eventCode, const int* key, const string* value, const string* metadata)
			{
				OnData(eventCode, key, value, metadata);
				Beg4Moar();
			});
	}

	void OnData(int eventCode, const int* key, const string* value, const string* metadata)
	{
		cout << "wnp_next " << eventCode 
			<< ", key: " << *key
			<< ", value: " << (value ? *value : "(null)")
			<< ", metadata: " << (metadata ? *metadata : "(null)")
			<< endl;
	}
};

void QueueModificationsWhileConnecting()
{
	asio::io_service io_service;

	tio::AsyncConnection cm(io_service);
	cm.Connect("localhost", 2606);

	auto errorHandler = [](const async_error_info& error)
	{
		cout << "ERROR: " << endl << endl;
	};

	tio::containers::async_map<string, string> m;

	m.create(&cm, "am", "volatile_map", NULL, errorHandler);

	m.set("abc", "abc", nullptr, [](){}, errorHandler);

	m.subscribe("0",
		[](){},
		errorHandler,
		[](int code, const string* k, const string* v, const string* m)
		{
			if(code == TIO_EVENT_SNAPSHOT_END)
				cout << "snapshot end" << endl;
			else
				cout << code << " - k=" << *k << ", v=" << *v << endl;
		});

	for(int a = 0 ; a < 10 ; a++)
	{
		string key = lexical_cast<string>(a);
		cout << "setting \"" << key << "\"" << endl;
		m.set(key, lexical_cast<string>(a), NULL, [key](){cout << "\"" << key << "\" set" << endl; }, errorHandler);
	}

	io_service.run();
}


void AsyncClientTest()
{
	asio::io_service io_service;

	//tio::AsyncConnection cm(io_service);
	tio::AsyncConnection cm(tio::AsyncConnection::UseOwnThread);
	cm.Connect("localhost", 2606);

	auto errorHandler = [](const async_error_info& error)
	{
		cout << "ERROR: " << endl << endl;
	};

	tio::containers::async_map<string, string> m, m2, m3;

	auto do_a_lot_of_stuff = 
		[&]()
		{
			m.propset("a", "10",
				[]() { cout << "propset done" << endl; },
				errorHandler);

			m.propget("a",
				[](const string& key, const string& value) 
				{ 
					cout << "propget key=" << key << ", value=" << value << endl; 
				},
				errorHandler);

			m.subscribe("",
				[]()
				{
					cout << "subscribed" << endl;
				},
				errorHandler,
				[](int eventCode, const string* key, const string* value, const string* metadata)
				{
					cout << "event " << eventCode 
						<< ", key: " << (key ? *key : "(null)")
						<< ", value: " << (value ? *value : "(null)")
						<< ", metadata: " << (metadata ? *metadata : "(null)")
						<< endl;
				}
			);

			for(int a = 0 ; a < 10 ; a++)
			{
				m.set("a", "b", nullptr,
					[]()
					{
						cout << "set command done" << endl;
					},
					errorHandler);

				m.get("a", 
					[](const string* key, const string* value, const string* metadata)
					{
						cout << "get: " << *key << "=" << *value << endl;
					},
					errorHandler);

				m.set(lexical_cast<string>(a), lexical_cast<string>(a), NULL, NULL, errorHandler);
			}
		};

	m.create(&cm, "am", "volatile_map", do_a_lot_of_stuff, errorHandler);

	m3.open(&cm, "am", 
		[&]()
		{
			m3.query(nullptr, nullptr,[](){}, errorHandler,
				[](int eventCode, const string* key, const string* value, const string* metadata)
				{
					cout << "query " << eventCode 
						<< ", key: " << (key ? *key : "(null)")
						<< ", value: " << (value ? *value : "(null)")
						<< ", metadata: " << (metadata ? *metadata : "(null)")
						<< endl;
				}
			);
		}
		, errorHandler);


	tio::containers::async_list<string> l1;
	WnpNextHandler wnpHandler;
	
	l1.create(&cm, "l1", "volatile_list",
		[&]()
		{
			l1.push_back("1", nullptr, NULL, NULL);
			l1.push_back("2", nullptr, NULL, NULL);
			l1.push_back("3", nullptr, NULL, NULL);
			l1.push_back("4", nullptr, NULL, NULL);
			l1.push_back("5", nullptr, NULL, NULL);
			l1.push_back("6", nullptr, NULL, NULL);

			wnpHandler.Start(&l1);
		},
		errorHandler
	);

	if(cm.usingSeparatedThread())
	{
		io_service.post([]()
		{
			for(bool b = true ; b ; )
				Sleep(100);
		});
	}

	io_service.run();

	cm.Disconnect();

	return;
}





void TestGroupCpp()
{
	tio::Connection cn;
	vector<shared_ptr<tio::containers::list<string>>> containers;

	static const int CONTAINER_COUNT = 5;
	static const int ITEM_COUNT = 5;
	static const char* GROUP_NAME = "test_group";

	cn.Connect("localhost");

	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		string name = "container_" + lexical_cast<string>(a + 1);

		shared_ptr<tio::containers::list<string>> container(new tio::containers::list<string>());

		container->create(&cn, name);

		containers.push_back(container);

		for(int b = 0 ; b < ITEM_COUNT ; b++)
			container->push_back(lexical_cast<string>(b));

		cn.AddToGroup(GROUP_NAME, container->name());
	}
}


 map<string, int> g_nextExpectedValuePerContainer;

void group_test_callback(void* cookie, const char* group_name, const char* container_name, unsigned int handle,
						 unsigned int event_code, const struct TIO_DATA* k, const struct TIO_DATA* v, const struct TIO_DATA* m)
{
	assert((int)cookie == 10);

	

	if(event_code != TIO_COMMAND_PUSH_BACK)
		return;

	static bool log = false;

	if(log)
	{
		TIO_DATA value_copy;

		tiodata_init(&value_copy);
		tiodata_copy(v, &value_copy);
		tiodata_convert_to_string(&value_copy);

		cout << "group:" << group_name 
			<< ", name:" << container_name 
			<< ", event_code: " << event_code 
			<< ", value: " << value_copy.string_ 
			<< endl;

		tiodata_set_as_none(&value_copy);
	}

	ASSERT(v->data_type == TIO_DATA_TYPE_INT);
	ASSERT(g_nextExpectedValuePerContainer[container_name] == v->int_);

	g_nextExpectedValuePerContainer[container_name] = v->int_ + 1;

	return;
}


void test_group_subscribe()
{
	TIO_CONNECTION* cn;
	static const int CONTAINER_COUNT = 20 * 1000;
	static const int ITEM_COUNT_BEFORE = 30;
	const char* group_name = "test_group";
	vector<TIO_CONTAINER*> containers;

	tio_connect("localhost", 2606, &cn);

	cout << "creating and filling containers..." << endl;
	
	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		string name = "container_";
		name += lexical_cast<string>(a);

		TIO_CONTAINER* container;

		tio_create(cn, name.c_str(), "volatile_list", &container);
		containers.push_back(container);
		tio_container_clear(container);

		tio_begin_network_batch(cn);

		for(int b = 0 ; b < ITEM_COUNT_BEFORE ; b++)
		{
			TIO_DATA value;

			tiodata_init(&value);
			tiodata_set_int(&value, b);

			tio_container_push_back(containers[a], NULL, &value, NULL);
		}

		tio_finish_network_batch(cn);

		if(a % 100 == 0)
		{
			cout << name << endl;
		}
	}

	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		tio_group_add(cn, group_name, containers[a]->name);
	}

	cout << "subscribing..." << endl;

	tio_group_set_subscription_callback(cn, &group_test_callback, (void*)10);
	tio_group_subscribe(cn, group_name, "0");

	tio_dispatch_pending_events(cn, 0xFFFFFFFF);

	cout << "adding more records..." << endl;

	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		for(int b = 0 ; b < ITEM_COUNT_BEFORE ; b++)
		{
			TIO_DATA value;

			tiodata_init(&value);
			tiodata_set_int(&value, b + ITEM_COUNT_BEFORE);

			tio_container_push_back(containers[a], NULL, &value, NULL);

			tio_dispatch_pending_events(cn, 0xFFFFFFFF);
		}

		if(a % 1000 == 0)
		{
			string name = "container_";
			name += lexical_cast<string>(a);

			cout << name << endl;
		}
	}

	tio_dispatch_pending_events(cn, 0xFFFFFFFF);

	for(auto i = g_nextExpectedValuePerContainer.begin() ; i != g_nextExpectedValuePerContainer.end() ; ++i)
	{
		assert(i->second == ITEM_COUNT_BEFORE * 2);

		if(i->second != ITEM_COUNT_BEFORE * 2)
			cout << "ERROR IN CONTAINER EVENT COUNT. value=" << i->second
			<< ", expected:" << ITEM_COUNT_BEFORE * 2 << endl;
	}
}

int _tmain(int argc, _TCHAR* argv[])
{
	test_group_subscribe();

	return 0;
}
