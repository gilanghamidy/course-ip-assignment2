#include <mosquittopp.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <string>
#include <sstream>
#include <vector>
#include <iterator>
#include <algorithm>
#include <list>
#include <mutex>
#include <thread>
#include <chrono>
#include <exception>
#include <future>
#include <fstream>
#include <map>
#include <iomanip>

namespace mqtt::server {

struct data {
	uint32_t board_id;
	uint64_t timestamp;
	double temp_avg;
};

struct board {
	uint32_t board_id;
	std::map<uint64_t, data> data_table;
};

std::map<uint32_t, board> board_table;

void fill_data(char const* data_file) { 
	std::ifstream str(data_file, std::ios::in);
	std::string line;
	std::getline(str, line);

	while(!str.eof()) {
		std::getline(str, line);

		if(line.length() == 0)
			continue;

		auto firstPos = line.find(',', 0);
		auto timestr = line.substr(0, firstPos);

		std::tm t{};
		std::istringstream ss(timestr);
		ss >> std::get_time(&t, "%m/%d/%Y %I:%M:%S %p");
		auto timestamp = std::mktime(&t);

		decltype(firstPos) lastPos = firstPos;
		for(int i = 0; i < 3; i++) {
			lastPos = firstPos + 1;
			firstPos = line.find(',', lastPos);
		}

		uint32_t boardId = std::stoi(line.substr(lastPos, firstPos));

		
		for(int i = 0; i < 3; i++) {
			lastPos = firstPos + 1;
			firstPos = line.find(',', lastPos);
		}

		auto tempAvg = std::stod(line.substr(lastPos, firstPos));
		auto iter = board_table.find(boardId);
		if(iter == board_table.end()) {
			auto inserted = board_table.emplace(boardId, board{ boardId, {} });
			iter = inserted.first;
		}
		auto& data_table = iter->second.data_table;

		
		auto iter2 = data_table.find((uint64_t)timestamp);
		if(iter2 == data_table.end()) {
			auto inserted = data_table.emplace((uint64_t)timestamp, data { boardId, (uint64_t)timestamp, tempAvg });
			iter2 = inserted.first;
		} else {
			iter2->second.board_id = boardId;
			iter2->second.timestamp = (uint64_t) timestamp;
			iter2->second.temp_avg = tempAvg;
		}
	}
}


class publisher_connection : public mosqpp::mosquittopp {
private:
	char const* addr;
	board const& board_data;
	std::promise<bool> connect_promise;
	std::promise<int> subscribe_promise;

	bool sending { false };
	std::string id;
	std::string topic_main;

public:
	publisher_connection(char const* addr, board const& board_data);
	virtual ~publisher_connection() { }
	void run();
	virtual void on_disconnect(int rc) { std::cout << "Disconnected..." << std::endl; sending = false; }
	virtual void on_publish(int mid) { std::cout << "Published..." << std::endl; this->disconnect(); }
	virtual void on_connect(int rc) { std::cout << "Connected: " << rc << std::endl; }
	/*
	virtual void on_connect_with_flags(int rc, int flags);
	virtual void on_message(const struct mosquitto_message * message);
	virtual void on_unsubscribe(int mid);
	virtual void on_log(int level, const char * str);
	virtual void on_error();
	*/
};
 
publisher_connection::publisher_connection(char const* addr, board const& board_data) : 
	board_data(board_data), addr(addr) {
	this->id = std::to_string(board_data.board_id);
	this->topic_main = "sensor/";
	this->topic_main.append(this->id);

	this->tls_set("ca.crt");
}

void publisher_connection::run() {
	auto data_ptr = board_data.data_table.begin();
	auto data_end = board_data.data_table.end();

	while(true) {
		if(!sending) {
			// Sleep randomly
			int sleep = (std::rand() % 5) + 1;
			auto sleep_duration = std::chrono::seconds(sleep);
			std::this_thread::sleep_for(sleep_duration);

			// Send a new data
			if(data_ptr == data_end)
				data_ptr = board_data.data_table.begin();

			sending = true;
			this->connect(addr);

			auto data_obj = data_ptr->second;
			this->publish(nullptr, this->topic_main.c_str(), sizeof(data_obj), &data_obj, 1);
		}
		this->loop();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}


}


int main(int argc, char** argv) {
	if(argc <= 1) {
		std::cout << "Supply target broker!" << std::endl;
		return 1;
	}

	auto addr = argv[1];

	mosqpp::lib_init();
		
	using namespace mqtt::server;
	mqtt::server::fill_data("sensor_data.csv");
	std::cout << board_table.size() << std::endl;

	std::vector<std::thread> threads;
    std::transform(board_table.begin(), board_table.end(), std::back_inserter(threads),
					[addr] (auto& d) -> std::thread {
						return std::thread { [addr, &d] { publisher_connection { addr, d.second }.run(); } }; 
					});

	std::for_each(threads.begin(), threads.end(), [] (auto& t) { t.join(); });

	mosqpp::lib_cleanup();


	return 0;
}