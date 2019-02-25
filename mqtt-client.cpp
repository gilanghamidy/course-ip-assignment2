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
#include <sqlite3.h>

namespace mqtt::client {

class client_connection : public mosqpp::mosquittopp {
private:
	char const* addr;
	std::mutex* main_thread_lock { nullptr };
	bool connected { false };

	std::promise<bool> connect_promise;
	std::promise<int> subscribe_promise;
public:
	client_connection(char const* addr);
	virtual ~client_connection() { }
	void run();
	std::future<bool> start_connection();
	std::future<int> subscribe(std::string const& topic, int qos = 0);
	std::future<int> unsubscribe(std::string const& topic);
	bool is_connected() const { return connected; }
	virtual void on_connect(int rc);
	virtual void on_disconnect(int rc);
	virtual void on_subscribe(int mid, int qos_count, int const* granted_qos);
	virtual void on_message(const struct mosquitto_message * message);
	virtual void on_unsubscribe(int mid);
	/*
	virtual void on_connect_with_flags(int rc, int flags);
	virtual void on_publish(int mid);
	virtual void on_message(const struct mosquitto_message * message);
	virtual void on_unsubscribe(int mid);
	virtual void on_log(int level, const char * str);
	virtual void on_error();
	*/
};

struct data {
	uint32_t board_id;
	uint64_t timestamp;
	double temp_avg;
};

class database_handler {
private:
	std::mutex queue_lock;
	std::list<data> queue;
	std::thread database_thread;
	sqlite3* db { nullptr };
	bool select(data& ref);
	bool shutdown_signal { false };
public:
	void run();
	void thread_proc();
	void shutdown();
	void enqueue(data const& d);
};

database_handler db;

}

using namespace mqtt::client;

void database_handler::run() {
	int err = 0;
	err = sqlite3_open("ClientDB.dat", &this->db);
	if(err != SQLITE_OK)
		throw std::exception();
	
	std::thread t(&database_handler::thread_proc, this);
	this->database_thread = std::move(t);
}

char insert_sql[] = "INSERT INTO data VALUES(?, ?, ?)";

bool database_handler::select(data& ref) {
	{
		std::lock_guard guard(this->queue_lock);

		if(this->queue.empty())
			return false;

		ref = this->queue.front();
		this->queue.pop_front();
		return true;
	}
}

void database_handler::enqueue(data const& d) {
	{
		std::lock_guard guard(this->queue_lock);
		this->queue.push_back(d);
	}
}

void database_handler::shutdown() {
	shutdown_signal = true;
	this->database_thread.join();
}

void database_handler::thread_proc() {
	sqlite3_stmt* stmt = nullptr;
	int err = 0;
	err = sqlite3_prepare(this->db, insert_sql, sizeof(insert_sql), &stmt, nullptr);
	if(err != SQLITE_OK)
		throw std::exception();

	while(true) {
		data d;
		if(select(d)) {
			sqlite3_reset(stmt);	
			sqlite3_bind_int(stmt, 1, d.board_id);
			sqlite3_bind_int(stmt, 2, d.timestamp);
			sqlite3_bind_double(stmt, 3, d.temp_avg);
			err = sqlite3_step(stmt);
			if(err != SQLITE_DONE)
				std::cerr << "Error inserting: " << err << std::endl;
		} else if(shutdown_signal) {
			break;
		} else {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	}
	sqlite3_close(this->db);
	this->db = nullptr;
	this->shutdown_signal = false;
}



void draw_prompt();

client_connection::client_connection(char const* addr) : addr(addr) {

}

void client_connection::on_message(const struct mosquitto_message* message) {
	data d = *((data*)message->payload);

	std::cout << "\n" << (uint64_t)this
			  << "Topic: " << message->topic
			  << " Message: " << d.board_id << ", " << d.timestamp << ", " << d.temp_avg
			  << std::endl;
	db.enqueue(d);
}

std::future<int> client_connection::subscribe(std::string const& topic, int qos) {
	std::promise<int> new_promise;
	auto ret = new_promise.get_future();
	this->subscribe_promise = std::move(new_promise);
	this->mosquittopp::subscribe(nullptr, topic.c_str(), qos);
	return ret;
}

std::future<int> client_connection::unsubscribe(std::string const& topic) {
	std::promise<int> new_promise;
	auto ret = new_promise.get_future();
	this->subscribe_promise = std::move(new_promise);
	this->mosquittopp::unsubscribe(nullptr, topic.c_str());
	return ret;
}


void client_connection::on_subscribe(int mid, int qos_count, int const* granted_qos) {
	this->subscribe_promise.set_value(mid);
}

void client_connection::on_unsubscribe(int mid) {
	this->subscribe_promise.set_value(mid);
}

std::future<bool> client_connection::start_connection() {
	std::promise<bool> new_promise;
	auto ret = new_promise.get_future();
	this->connect_promise = std::move(new_promise);
	this->connect_async(addr);
	return ret;
}

void client_connection::on_connect(int rc) {
	if(rc == 0) {
		this->connected = true;
		this->connect_promise.set_value(true);
	} else {
		this->connected = false;
		this->connect_promise.set_value(false);
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
	std::cout << "[nyamuk " << (active_connection == 0 ? "disconnected" : "connected") << "]$ " << std::flush;
}

template<typename T>
T waiter(std::future<T>&& m) {
	using namespace std::chrono_literals;
	while(true) {
		std::cout << '.' << std::flush;
		if(m.wait_for(100ms) == std::future_status::ready)
			return m.get();
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

	db.run();

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
			db.shutdown();
			std::cout << "Bye!" << std::endl;
			break;
		} else if(command == "open") {
			// Open a new connection
			std::cout << "Trying to open a new connection";
			auto& new_conn = connections.emplace_back(broker_addr);
			new_conn.tls_set("ca.crt");
			new_conn.loop_start();
			if(waiter(new_conn.start_connection())) {
				std::cout << " Success. Connection ID: " << active_connection++ << std::endl;
			} else {
				std::cout << " Failed" << std::endl;
				connections.pop_back(); // Remove the object
			}
		} else if(command == "sub") {
			if(tokens.size() < 2) {
				std::cout << "Please enter the topic" << std::endl;
				continue;
			}

			auto connection = connections.end();
			connection--;

			if(tokens.size() == 2) {
				std::cout << "No connection ID specified. Using last created connection.";
			} else {
				try {
					auto index = std::stoi(tokens[2]);
					if(index >= connections.size()) {
						std::cout << "Invalid connection index to connect to. Use 'list' to list all active connection." << std::endl;
						continue;
					}

					connection = connections.begin();
					std::advance(connection, index);
					
				} catch(std::invalid_argument const& e) {
					std::cout << "Invalid connection index to connect to. Only integer is allowed." << std::endl;
					continue;
				}
			}
			int mid = waiter(connection->subscribe(tokens[1]));
			std::cout << " Success. Message ID: " << mid << std::endl;
			
		} else if(command == "unsub") {
			if(tokens.size() < 2) {
				std::cout << "Please enter the topic" << std::endl;
				continue;
			}

			auto connection = connections.end();
			connection--;

			if(tokens.size() == 2) {
				std::cout << "No connection ID specified. Using last created connection.";
			} else {
				try {
					auto index = std::stoi(tokens[2]);
					if(index >= connections.size()) {
						std::cout << "Invalid connection index to connect to. Use 'list' to list all active connection." << std::endl;
						continue;
					}

					connection = connections.begin();
					std::advance(connection, index);
					
				} catch(std::invalid_argument const& e) {
					std::cout << "Invalid connection index to connect to. Only integer is allowed." << std::endl;
					continue;
				}
			}
			int mid = waiter(connection->unsubscribe(tokens[1]));
			std::cout << " Success. Message ID: " << mid << std::endl;
		}
	}

	

	mosqpp::lib_cleanup();

	return 0;
}
