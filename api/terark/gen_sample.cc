

#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

using namespace std;

const char* path = "./samples_large.txt";
std::string gen() {
	char arr[51] = { 0 };
	int sz = max(std::rand() % 50, 4);
	for (int i = 0; i < sz; i++) {
		// ascii: 33 ~ 126
		int random_variable = std::rand() % 94 + 33;
		arr[i] = random_variable;
	}
	return arr;
}

int main() {
	std::srand(std::time(0)); // use current time as seed for random generator
	ofstream fo(path);
	for (int i = 0; i < 1000 * 1000; i++) {
		std::string key = gen();
		std::string val = gen();
		fo << "key: " << key << '\n';
		fo << "val: " << val << '\n';
	}
	//std::cout << "Random value on [0 " << RAND_MAX << "]: " 
	//        << random_variable << '\n';
	return 0;
}
