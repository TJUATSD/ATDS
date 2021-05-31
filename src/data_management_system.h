#ifndef _DATA_MANAGEMENT_SYSTEM_H_
#define _DATA_MANAGEMENT_SYSTEM_H_ 1
/*
    store data with mysql
*/

#include "data_transform_system/table.h"
#include <string>
#include <cstring>
#include <iomanip>
#include <mysql.h>

using std::cout;
using std::cin;
using std::endl;
using std::string;
using std::to_string;

class Data_management_system{
    public:
        MYSQL conn;

        Data_management_system();
        ~Data_management_system();
        void data_import(Table table);
        Table dataset_export();
        void data_export(char _dir[], int size);
        void data_export2(char _dir[], int size);
        void disconnect();
        void clear_dataset();
};

#endif // DATA_MANAGEMENT_SYSTEM_H_INCLUDED
