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

namespace mqtt::client {

class client_connection : public mosqpp::mosquittopp {
private:
	char const* addr;
	std::mutex* main_thread_lock { nullptr };
	bool connected { false };
public:
	client_connection(char const* addr);
	virtual ~client_connection() { }
	void run();
	void start_connection(std::mutex& lock);
	bool is_connected() const { return connected; }
	virtual void on_connect(int rc);
	virtual void on_disconnect(int rc);
	/*
	virtual void on_connect_with_flags(int rc, int flags);
	virtual void on_publish(int mid);
	virtual void on_message(const struct mosquitto_message * message);
	virtual void on_subscribe(int mid, int qos_count, const int * granted_qos);
	virtual void on_unsubscribe(int mid);
	virtual void on_log(int level, const char * str);
	virtual void on_error();
	*/
};

}

using namespace mqtt::client;

client_connection::client_connection(char const* addr) : addr(addr) {

}

void client_connection::start_connection(std::mutex& lock) {
	this->main_thread_lock = &lock;
	lock.lock();
	this->connect_async(addr);
}

void client_connection::on_connect(int rc) {
	this->connected = true;
	if(this->main_thread_lock) {
		this->main_thread_lock->unlock();
		this->main_thread_lock = nullptr;
	}
}

void client_connection::on_disconnect(int rc) {
	this->connected = false;
}

void client_connection::run() {
	auto res = this->connect(addr);
	if(res != MOSQ_ERR_SUCCESS) {
		auto err = std::strerror(errno);
		std::cout << "Error while trying to connect: " << err << std::endl;
		return;
	}

	std::cout << "Connection successfull!" << std::endl;
	return;
}

int active_connection = 0;

void draw_prompt() {
	std::cout << "[nyamuk " << (active_connection == 0 ? "disconnected" : "connected") << "]$ ";
}

void waiter(std::mutex& m) {
	using namespace std::chrono_literals;
	while(true) {
		std::cout << '.';
		if(m.try_lock())
			return;
		std::this_thread::sleep_for(100ms);
	}
}

int main(int argc, char** argv) {
	std::cout << "Welcome to Nyamuk! An MQTT testing program by Gilang Hamidy" << std::endl;

	if(argc < 2) {
		std::cout << "Please supply the address of the broker on command argument!" << std::endl;
		return -1;
	}

	auto broker_addr = argv[1];

	mosqpp::lib_init();

	std::list<client_connection> connections;

	while(true)
	{
		// Obtain command
		draw_prompt();
		std::string line_command;
		std::getline(std::cin, line_command);

		if(line_command.length() == 0)
			continue;

		std::istringstream iss(line_command);
		std::vector<std::string> tokens{ std::istream_iterator<std::string>{ iss }, std::istream_iterator<std::string>{}};

		auto& command = tokens[0];
		std::transform(command.begin(), command.end(), command.begin(), [] (unsigned char c) -> unsigned char { return std::tolower(c); });

		if(command == "quit") {
			for(auto& connection : connections) {
				connection.disconnect();
				connection.loop_stop();
			}
			std::cout << "Bye!" << std::endl;
			break;
		} else if(command == "open") {
			// Open a new connection
			std::cout << "Trying to open a new connection";
			auto& new_conn = connections.emplace_back(broker_addr);
			new_conn.loop_start();
			std::mutex lock;
			new_conn.start_connection(lock);
			waiter(lock);
			if(new_conn.is_connected()) {
				std::cout << " Success. Connection ID: " << active_connection++ << std::endl;
			} else {
				std::cout << " Failed" << std::endl;
				connections.pop_back(); // Remove the object
			}
		}
	}

	mosqpp::lib_cleanup();

	return 0;
}
