#ifndef _Table_H_
#define _Table_H_ 1

// using namespace std;
#include <iostream>
#include <map>
#include <string>
#include <yaml-cpp/yaml.h>
#include <malloc.h>
#include <cstring>
#include <stdlib.h>

using std::string;
using std::endl;
using std::cout;
using std::map;
using std::pair;

#define max_string_column_element_size 30

typedef struct Column{
    char* name;
    char* unit;
    char* datatype;
    Column(){}
    struct Column & operator=(const struct Column & p){
        this->name = (char*)malloc(strlen(p.name)*sizeof(char)+2);
        this->unit = (char*)malloc(strlen(p.unit)*sizeof(char)+2);
        this->datatype = (char*)malloc(strlen(p.datatype)*sizeof(char)+2);
        strcpy(this->name, p.name);
        strcpy(this->unit, p.unit);
        strcpy(this->datatype, p.datatype);
        return *this;
    }
}col;

typedef struct Meta_data{
    char* key;
    char* value;
    Meta_data(){}
    // Meta_data(string _key, string _value): key(_key), value(_value){}
    struct Meta_data & operator=(const struct Meta_data & p){
        this->key = (char*)malloc(strlen(p.key)*sizeof(char)+2);
        this->value = (char*)malloc(strlen(p.value)*sizeof(char)+2);
        strcpy(this->key, p.key);
        strcpy(this->value, p.value);
        return *this;
    }
}meta_data;

class Table{
	public:
		Table(YAML::Node config);
		Table();
		~Table();
		Table(const Table& p);
        Table& operator=(const Table& p);


		// constructor flag
        int flag=-1;
		// metadata(dict), column description(dict), data(array)
		// metadata
		meta_data* meta;
		int meta_len;

		// column description
		col* col_desc;
		int col_len;
		map<string, int> colname_index; // map of name and order in col_desc

		// data
		void** data;
		int row_len;

		// map of name and order in data[i]
		map<string, int> name_index;
		map<int, string> index_name;

		// map of order in data[i] and type of data[i]
		// type defination as following
		// 0��string��1��double��2��float��3��short��4��int
		map<int, int> col_type;


		string flatten(YAML::Node in);
		void serialization(meta_data meta);
		void serialization(col column);
		void assign(meta_data &meta, string _key, string _value);
		void assign(col &column, string _name, string _unit, string _datatype);
		void test();
		void add_name_index(string t, string delimiter);
		void def_data_shape(int _row_len);
		void add_data(string t, string delimiter);
		string find_meta_value (string _key) const;
};

#endif



