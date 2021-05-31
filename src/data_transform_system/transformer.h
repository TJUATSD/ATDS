#ifndef _Transformer_H_
#define _Transformer_H_ 1

#include <iostream>
#include <yaml-cpp/yaml.h>
#include <assert.h>
#include <direct.h>
#include <io.h>
#include <fstream>
#include <string>
#include <regex>
#include "table.h"

using std::cout;
using std::cin;
using std::endl;
using std::string;
using std::regex;
using std::regex_match;


class Transformer{
	public:
		Transformer();
		~Transformer(){};

		void FITS_to_table();
		Table csv_to_table(char file[], int len);
		void table_to_FITS();
		void table_to_csv();
};

#endif
