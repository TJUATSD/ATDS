#ifndef _Data_Transform_System_H_
#define _Data_Transform_System_H_ 1

// using namespace std;
#include <iostream>
#include "data_transform_system/table.h"
#include "data_transform_system/transformer.h"
#include <cstring>
#include <io.h>
#include <string>
#include <stack>
#include <direct.h>
#include <time.h>
#include <pthread.h>
#include "data_transform_system/table.h"
#include "data_management_system.h"
//#include "exeptions.h"

using std::cout;
using std::cin;
using std::endl;
using std::string;

class Data_transform_system{
	public:
		Data_transform_system();
		Data_transform_system(char dir[]);
		~Data_transform_system();
		void init();
		void data_import(char dir[], int size);
		void data_export(char dir[], int size);
		void dataset_export(char dir[], int size);

	private:
		char dir[1024];

};

#endif
