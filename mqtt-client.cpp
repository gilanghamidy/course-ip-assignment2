#include <mosquittopp.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <string>
#include <list>

namespace mqtt::client {

class client_connection : mosqpp::mosquittopp {
private:
	char const* addr;
public:
	client_connection(char const* addr);
	virtual ~client_connection() { }
	void run();
	virtual void on_connect(int rc);
	virtual void on_connect_with_flags(int rc, int flags);
	virtual void on_disconnect(int rc);
	virtual void on_publish(int mid);
	virtual void on_message(const struct mosquitto_message * message);
	virtual void on_subscribe(int mid, int qos_count, const int * granted_qos);
	virtual void on_unsubscribe(int mid);
	virtual void on_log(int level, const char * str);
	virtual void on_error();
};

}

using namespace mqtt::client;

client_connection::client_connection(char const* addr) : addr(addr) {

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
		std::cout << "[nyamuk] $ ";
		std::string line_command;
		std::getline(std::cin, line_command);

		if(line_command.length() == 0)
			continue;

		
	}

	mosqpp::lib_cleanup();

	return 0;
}