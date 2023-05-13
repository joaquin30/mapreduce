#include <iostream>
#include <sstream>
using namespace std;

int main() {
	string key;
	int value = 0;
	for(string in; getline(cin, in);) {
		istringstream ss(in);
		string k;
		int v;
		ss >> k >> v;
		if (key.empty())
			key = k;
		value += v;
	}
	cout << key << "\t" << value << "\n";
}

