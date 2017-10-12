
#include <iostream>

const char *gitversion = "dd7303b511e64178d530ba369db87172302778b2";

class TellMyVersion {
 public:
	TellMyVersion() {
		std::cout << "git version is: " << gitversion << std::endl;
	}
};

TellMyVersion myversion;
