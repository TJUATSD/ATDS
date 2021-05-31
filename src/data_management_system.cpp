#include "data_management_system.h"
#include <mysql.h>

#define Tconn &(this->conn)
#define dataset_name_max_length 64
#define meta_name_max_length 128
#define meta_col_desc_name_max_length 128
#define meta_data_name_max_length 128
#define col_desc_name_max_length 30
#define col_desc_datatype_max_length 10
#define col_desc_unit_max_length 10

void Data_management_system::clear_dataset(){
    mysql_init(Tconn);

    MYSQL_RES* res;
    MYSQL_FIELD field;
    MYSQL_ROW row;
    int ret;

    if (!mysql_real_connect(Tconn, "localhost", "root", "123456", "mysql", 3306, NULL, 0)){
        fprintf(stderr, "[Error] Failed to connect to database: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
    }else{
        // cout << "Successfully connected to Database: mysql.\n";

        // check if dataset, meta��column��data table exist
        // check dataset table
        string query = "drop database if exists astro_data;";
        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
        }
        query = "create database astro_data;";
        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
        }
    }
    mysql_close(Tconn);
}

Data_management_system::Data_management_system(){

}

void Data_management_system::data_export2(char _dir[], int size){

    string dir = _dir;
    string target = "target.csv";
    string tmp = dir + "/" + target;
    FILE* outfile = fopen(tmp.c_str(), "w");


    MYSQL conn;
    mysql_init(&conn);

    MYSQL_RES* res;
    MYSQL_FIELD field;
    MYSQL_ROW row;
    int ret;
    string query;
    string meta_table;
    string data_table;
    string OBJID;
    string DEC;
    string RA;

    // 0.1 connect astro_data database
    if (!mysql_real_connect(&conn, "localhost", "root", "123456", "astro_data", 3306, NULL, 0)){
        fprintf(stderr, "[Error] Failed to connect to database: Error: %s\n", mysql_error(&conn));
        cout << stderr;
        return;
    }
    cout << "[Info] Successfully connected to Database: astro_data.\n";

    // 1.1 gain all meta table in dataset
    query = "select meta_table from dataset;";
    ret = mysql_real_query(&conn, query.c_str(), query.length());
    if (ret){
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
        cout << stderr;
        return;
    }

    res = mysql_store_result(&conn);
    row = mysql_fetch_row(res);

    if (row == NULL){
        cout << "[Warning] NO meta_table.\n";
        return;
    }else{
        // 1.2 get all rows in meta table, assign data_table
        // cout << "[Info] Get meta_table.\n";
        for (;row != NULL; row = mysql_fetch_row(res)){
            meta_table = row[0];
            query = "select OBJID, RA, _DEC, data_table from "+meta_table+";";
            ret = mysql_real_query(&conn, query.c_str(), query.length());
            if (ret){
                fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
                cout << stderr;
                return;
            }
            MYSQL_RES* meta_res = mysql_store_result(&conn);
            MYSQL_ROW meta_row = mysql_fetch_row(meta_res);
            if (meta_row == NULL){
                cout << "[Warning] no data_table.\n";
                continue;
            }else{
                // cout << "[Info] Get data_table.\n";
                // 1.3 write data from data_table to file.
                string pre_data_table = "";
                for (;meta_row != NULL; meta_row = mysql_fetch_row(meta_res)){
                    OBJID = meta_row[0];
                    RA = meta_row[1];
                    DEC = meta_row[2];
                    data_table = meta_row[3];
                    // 2021-5-29 �޸�
                    if (data_table == pre_data_table){
                        continue;
                    }
                    pre_data_table = data_table;

                    query = "select DATETIME, round(MJD, 11), round(X, 16), round(Y, 17), \
                            round(RA, 16), round(_DEC, 16), round(FLUX, 16), round(FLUX_ERR, 6), \
                            round(MAG_AUTO, 6), round(MAGERR_AUTO, 6), round(BACKGROUND, 7), \
                            round(FWHM, 6), round(A, 6), round(B, 6), round(THETA, 6), FLAGS, \
                            round(R50, 6), round(MAG, 4), round(MAGERR, 7) \
                            from "+data_table+";";


                    ret = mysql_real_query(&conn, query.c_str(), query.length());
                    if (ret){
                        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
                        cout << stderr;
                        return;
                    }
                    MYSQL_RES* data_res = mysql_store_result(&conn);
                    MYSQL_ROW data_row = mysql_fetch_row(data_res);
                    if (data_row == NULL){
                        cout << "[Warining] No data in data_table.\n";
                        continue;
                    }else{
                        // cout << "[Info] Get data.\n";
                        // 1.4 write every row to file
                        fprintf(outfile, "#");
                        fprintf(outfile, OBJID.c_str());
                        fprintf(outfile, " ");
                        fprintf(outfile, RA.c_str());
                        fprintf(outfile, " ");
                        fprintf(outfile, DEC.c_str());
                        fprintf(outfile, "\n");
                        unsigned int num_fields = mysql_num_fields(data_res);
                        for (;data_row != NULL; data_row = mysql_fetch_row(data_res)){
                            //cout << count++ << endl;
                            for (int j=0; j<num_fields-1; ++j){
                                fprintf(outfile, data_row[j]);
                                fprintf(outfile, ",");
                            }
                            fprintf(outfile, data_row[num_fields-1]);
                            fprintf(outfile, "\n");
                        }
                    }
                    mysql_free_result(data_res);
                }
            }
            mysql_free_result(meta_res);
        }
    }
    mysql_free_result(res);// release resource
    fclose(outfile);
    mysql_close(&conn);
}

void Data_management_system::data_export(char _dir[], int size){

    string dir = _dir;
    string target = "target.csv";
    string tmp = dir + "/" + target;
    FILE* outfile = fopen(tmp.c_str(), "w");


    MYSQL conn;
    mysql_init(&conn);

    MYSQL_RES* res;
    MYSQL_FIELD field;
    MYSQL_ROW row;
    int ret;
    string query;
    string meta_table;
    string data_table;
    string OBJID;
    string DEC;
    string RA;

    // 0.1 connect astro_data database
    if (!mysql_real_connect(&conn, "localhost", "root", "123456", "astro_data", 3306, NULL, 0)){
        fprintf(stderr, "[Error] Failed to connect to database: Error: %s\n", mysql_error(&conn));
        cout << stderr;
        return;
    }
    cout << "[Info] Successfully connected to Database: astro_data.\n";

    // 1.1 gain all meta table in dataset
    query = "select meta_table from dataset;";
    ret = mysql_real_query(&conn, query.c_str(), query.length());
    if (ret){
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
        cout << stderr;
        return;
    }

    res = mysql_store_result(&conn);
    row = mysql_fetch_row(res);

    if (row == NULL){
        cout << "[Warning] NO meta_table.\n";
        return;
    }else{
        // 1.2 get all rows in meta table, assign data_table
        // cout << "[Info] Get meta_table.\n";
        for (;row != NULL; row = mysql_fetch_row(res)){
            meta_table = row[0];
            query = "select OBJID, RA, _DEC, data_table from "+meta_table+";";
            ret = mysql_real_query(&conn, query.c_str(), query.length());
            if (ret){
                fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
                cout << stderr;
                return;
            }
            MYSQL_RES* meta_res = mysql_store_result(&conn);
            MYSQL_ROW meta_row = mysql_fetch_row(meta_res);
            if (meta_row == NULL){
                cout << "[Warning] no data_table.\n";
                continue;
            }else{
                // cout << "[Info] Get data_table.\n";
                // 1.3 write data from data_table to file.
                string pre_data_table = "";
                for (;meta_row != NULL; meta_row = mysql_fetch_row(meta_res)){
                    OBJID = meta_row[0];
                    RA = meta_row[1];
                    DEC = meta_row[2];
                    data_table = meta_row[3];
                    // 2021-5-29 �޸�
                    if (data_table == pre_data_table){
                        continue;
                    }
                    pre_data_table = data_table;

                    query = "select DATETIME, round(MJD, 11), round(MAG, 4), round(MAGERR, 7) from "+data_table+";";

                    ret = mysql_real_query(&conn, query.c_str(), query.length());
                    if (ret){
                        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(&conn));
                        cout << stderr;
                        return;
                    }
                    MYSQL_RES* data_res = mysql_store_result(&conn);
                    MYSQL_ROW data_row = mysql_fetch_row(data_res);
                    if (data_row == NULL){
                        cout << "[Warining] No data in data_table.\n";
                        continue;
                    }else{
                        // cout << "[Info] Get data.\n";
                        // 1.4 write every row to file
                        fprintf(outfile, "#");
                        fprintf(outfile, OBJID.c_str());
                        fprintf(outfile, " ");
                        fprintf(outfile, RA.c_str());
                        fprintf(outfile, " ");
                        fprintf(outfile, DEC.c_str());
                        fprintf(outfile, "\n");
                        int count = 1;
                        for (;data_row != NULL; data_row = mysql_fetch_row(data_res)){
                            //cout << count++ << endl;
                            fprintf(outfile, data_row[0]);
                            fprintf(outfile, ",");
                            fprintf(outfile, data_row[1]);
                            fprintf(outfile, ",");
                            fprintf(outfile, data_row[2]);
                            fprintf(outfile, ",");
                            fprintf(outfile, data_row[3]);
                            fprintf(outfile, "\n");
                        }
                        //fprintf(outfile, "\n");
                    }
                    mysql_free_result(data_res);
                }
            }
            mysql_free_result(meta_res);
        }
    }
    mysql_free_result(res);// release resource
    fclose(outfile);
    mysql_close(&conn);
}

void Data_management_system::data_import(Table table){
    // import Table into database

    mysql_init(Tconn);

    MYSQL_RES* res;
    MYSQL_FIELD field;
    MYSQL_ROW row;
    int ret;
    string dataset_name;
    string meta_name;
    string col_desc_name;
    string data_name;
    string query;
    string objid = table.find_meta_value("OBJID");

    dataset_name = table.find_meta_value(string("DATASET"));
    if (dataset_name == string("")){
        cout << "[Error] 'DATASET' did't find.\n";
        return;
    }

    // 0.1 connect to astro_data dataset
    if (!mysql_real_connect(Tconn, "localhost", "root", "123456", "astro_data", 3306, NULL, 0)){
        fprintf(stderr, "[Error] Failed to connect to database: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    // cout << "[Info] Successfully connected to Database: astro_data.\n";

    // check if dataset, meta��column��data exist, create or insert

    // 1.1 check dataset table
    query = "show tables where Tables_in_astro_data = 'dataset';";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        // cout << "[Info] 'dataset' table found.\n";
    }else{
        // cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";

        query = "create table if not exists dataset( \
                dataset varchar("+ to_string(dataset_name_max_length)+"), \
                meta_table varchar("+to_string(meta_name_max_length)+"), \
                col_desc_table varchar("+to_string(col_desc_name_max_length)+"), \
                count bigint unsigned, PRIMARY KEY(dataset));";

        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] 'dataset' table created.\n";
        }
    }
    mysql_free_result(res);

    // 1.2 check current dataset existence
    query = "select meta_table, col_desc_table from dataset where dataset = \""+dataset_name+"\";";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        // cout << "[Info] Current dataset is found in 'dataset' table.\n";
        meta_name = row[0];
        col_desc_name = row[1];
    }else{
        meta_name = dataset_name+"_"+"meta";
        col_desc_name = dataset_name +"_"+"col_desc";

        // cout << "[Warning]Current dataset doesn't exist in 'dataset' table. Inserting.\n";

        query = "insert IGNORE into dataset(dataset, meta_table, col_desc_table, count) \
                VALUES \
                (\""+dataset_name+"\", \""+meta_name+"\", \""+col_desc_name+"\", 0);";

        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'dataset' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] Current dataset is inserted into 'dataset' table.\n";
        }
    }
    mysql_free_result(res);


    // 2.1 check meta table
    query = "show tables where Tables_in_astro_data = \""+meta_name+"\";";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        // cout << "[Info] '"+meta_name+"' table found.\n";
    }else{
        // cout << "[Warning] '"+meta_name+"' table doesn't exist. Creating.\n";


        query = "create table if not exists "+meta_name+"(";

        for (int i=0; i<table.meta_len; ++i){
            if (strcmp(table.meta[i].key, "DEC") == 0) query += "_";
            query += table.meta[i].key;
            query += " ";
            query += "varchar(";
            query += to_string(strlen(table.meta[i].value)*2);
            query += "), ";
        }

        query += "col_desc_table varchar(" + to_string(meta_col_desc_name_max_length) + "),";
        query += "data_table varchar(" + to_string(meta_data_name_max_length) + "),";
        query += "count bigint unsigned, PRIMARY KEY(DATASET, FIELD, OBJID)";
        query += ");";

        // cout << "### This query is: " << query << endl;
        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] 'meta' table created.\n";
        }
    }
    mysql_free_result(res);

    // 2.2 check current meta existence
    query = "select data_table from "+meta_name+" where DATASET = \"";
    query += table.find_meta_value("DATASET");
    query += "\" and FIELD = \"";
    query += table.find_meta_value("FIELD");
    query += "\" and OBJID = \"";
    query += table.find_meta_value("OBJID");
    query += "\" ;";

    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        data_name = row[0];
        // cout << "[Info] this meta information exists in meta table.\n";
        mysql_free_result(res);
    }else{
        mysql_free_result(res);

        query = "select COUNT(*) from "+meta_name+";";
        ret = mysql_real_query(Tconn, query.c_str(), query.length());

        if (ret){
            //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }
        res = mysql_store_result(Tconn);
        if (row = mysql_fetch_row(res)){
            // 2021-5-28 MODIFY data_name = meta_name + "_" + "data" + (row[0]);
            data_name = dataset_name + "_" + "data";
        }else{
            cout << "[Error] select count(*) return null.\n";
            return;
        }
        mysql_free_result(res);

        // cout << "[Warning] this meta information doesn't exist in meta table. Creating.\n";

        query = "insert IGNORE into " + meta_name + "(";
        for (int i=0; i<table.meta_len; ++i){
            if (strcmp(table.meta[i].key, "DEC") == 0) query += "_";
            query += table.meta[i].key;
            query += ", ";
        }
        query += "col_desc_table, data_table, count) ";
        query += "VALUES(";
        for (int i=0; i<table.meta_len; ++i){
            query += "\"";
            query += table.meta[i].value;
            query += "\"";
            query += ", ";
        }
        query += "\""+col_desc_name + "\", \"" + data_name + "\", 0)";
        query += ";";


        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] this meta information is inserted.\n";
        }
    }


    // 3.1 check col_desc table
    query = "show tables where Tables_in_astro_data = \""+col_desc_name+"\";";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        // cout << "[Info] 'col_desc' table found.\n";
    }else{
        // cout << "[Warning] "+col_desc_name+" table doesn't exist. Creating.\n";

        query = "create table if not exists "+col_desc_name;
        query += "(name varchar("+to_string(col_desc_name_max_length)+
                 "),datatype varchar("+to_string(col_desc_datatype_max_length)+
                 "), unit varchar("+to_string(col_desc_unit_max_length)+
                 "), PRIMARY KEY(name));";

        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] 'col_desc' table created.\n";
        }
    }
    mysql_free_result(res);

    // 3.2 check current col_desc information
    query = "select name from "+col_desc_name+";";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
        return;
    }
    res = mysql_store_result(Tconn);
    if (row = mysql_fetch_row(res)){
        // cout << "[Info] 'col_desc' table is filled.\n";
    }else{
        // cout << "[Warning] "+col_desc_name+" table doesn't filled with col_desc. Creating.\n";

        query = "insert IGNORE into "+col_desc_name+" (name, datatype, unit) VALUES";
        for (int i=0; i<table.col_len-1; ++i){
            query += "(";
            query += "\"";
            query += table.col_desc[i].name;
            query += "\", \"";
            query += table.col_desc[i].datatype;
            query += "\", \"";
            query += table.col_desc[i].unit;
            query += "\"";
            query += "), ";
        }
        query += "(";
        query += "\"";
        query += table.col_desc[table.col_len-1].name;
        query += "\", \"";
        query += table.col_desc[table.col_len-1].datatype;
        query += "\", \"";
        query += table.col_desc[table.col_len-1].unit;
        query += "\"";
        query += ");";

        ret = mysql_real_query(Tconn, query.c_str(), query.length());
        if (ret){
            //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
            fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
            cout << stderr;
            return;
        }else{
            // cout << "[Info] "+col_desc_name+" table is filled.\n";
        }
    }
    mysql_free_result(res);


    // 4.1 check data table
    query = "show tables where Tables_in_astro_data = \""+data_name+"\";";
    ret = mysql_real_query(Tconn, query.c_str(), query.length());
    if (ret){
        //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
        fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
        cout << stderr;
    }else{
        res = mysql_store_result(Tconn);
        if (row = mysql_fetch_row(res)){
            // cout << "[Info] "+data_name+" table found.\n";
        }else{
            // cout << "[Warning] "+data_name+" table doesn't exist. Creating.\n";

            query = "create table if not exists "+data_name+"(";
            for (int i=0; i<table.col_len; ++i){
                map<int, string>::iterator it1 = table.index_name.find(i);
                string name = it1->second;
                map<string, int>::iterator it2 = table.colname_index.find(name);
                string datatype = table.col_desc[it2->second].datatype;
                if (strcmp(name.c_str(), "DEC") == 0) query += "_";
                query += name;
                query += " ";
                if (strcmp(datatype.c_str(), "float64") == 0){
                    query += "DOUBLE";
                }else if (strcmp(datatype.c_str(), "float32") == 0){
                    query += "FLOAT";
                }else if (strcmp(datatype.c_str(), "int16") == 0){
                    query += "SMALLINT";
                }else if (strcmp(datatype.c_str(), "int32") == 0){
                    query += "INT";
                }else if (strcmp(name.c_str(), "DATETIME") == 0){
                    query += "DATETIME";
                }else{
                    cout << "[Error] not acceptable type of datatype "+datatype+".\n";
                    query += "VARCHAR(100)";
                }
                query += ", ";
            }
            query += "OBJID VARCHAR(20), ";
            query += "PRIMARY KEY(OBJID, DATETIME));";

            ret = mysql_real_query(Tconn, query.c_str(), query.length());
            if (ret){
                //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
                fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
                cout << stderr;
                return;
            }else{
                // cout << "[Info] "+data_name+" table created.\n";
            }
        }
        mysql_free_result(res);
    }

    // 4.2 insert data with IGNORE keyword

    mysql_autocommit(Tconn, 0);

    string query_head = "INSERT IGNORE into "+data_name+" (";

    for (int i=0; i<table.col_len; ++i){
        map<int, string>::iterator it1 = table.index_name.find(i);
        string name = it1->second;
        if (strcmp(name.c_str(), "DEC") == 0) query_head += "_";
        query_head += name;
        if (i < table.col_len-1) query_head += ", ";
    }
    query_head += ", OBJID) VALUES";

    string query_data;
    int cursor = 0;
    // cout << "[Info] Total data length is " << table.row_len << endl;
    for (int i=0; i<table.row_len; ++i){

        query_data += "( ";
        for (int j=0; j<table.col_len; ++j){
            map<int, int>::iterator it1 = table.col_type.find(j);
            int tmp_type = it1->second;
            // 0��string��1��double��2��float��3��short��4��int
            if (tmp_type == 0){
                query_data += "\"";
                query_data += (char*)((char**)table.data[j])[i];
                query_data += "\"";

            }else if (tmp_type == 1){
                std::stringstream ss;
                ss << std::setprecision(18) << ((double*)table.data[j])[i];
                query_data += ss.str();
                //query_data += to_string(((double*)table.data[j])[i]);

            }else if (tmp_type == 2){
                std::stringstream ss;
                ss << std::setprecision(10) << ((float*)table.data[j])[i];
                query_data += ss.str();
                //query_data += to_string(((float*)table.data[j])[i]);

            }else if (tmp_type == 3){
                query_data += to_string(((short*)table.data[j])[i]);

            }else if (tmp_type == 4){
                query_data += to_string(((int*)table.data[j])[i]);

            }else{
                cout << "[Error] When inserting data, datatype is not right.\n";
            }
            if (j<table.col_len-1) query_data += ", ";
        }
        query_data += ", "+objid+")";

        cursor++;
        if (cursor >= 10000 || i == table.row_len-1){

            query = query_head + query_data;
            ret = mysql_real_query(Tconn, query.c_str(), query.length());
            if (ret){
                //cout << "[Warning] 'meta' table doesn't exist. Creating.\n";
                fprintf(stderr, "[Error] Query failed: Error: %s\n", mysql_error(Tconn));
                cout << stderr;
            }

            cursor = 0;
            query_data = "";
        }else{
            query_data += ", ";
        }
    }
    mysql_commit(Tconn);

    mysql_close(Tconn);
}

Data_management_system::~Data_management_system(){
    //mysql_close(Tconn);
}

void Data_management_system::disconnect(){
    //mysql_close(Tconn);
}
