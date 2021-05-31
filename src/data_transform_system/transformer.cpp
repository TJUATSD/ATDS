#include "transformer.h"
Transformer::Transformer(){

};

void Transformer::FITS_to_table(){

};
Table Transformer::csv_to_table(char file[], int len){

    // read from ecsv, including ecsv statement，yaml header，csv data
    std::ifstream infile;
    infile.open(file, std::ios::in);
    if (!infile.is_open()){
        cout << "[Error] Can not open file: " << file << "\n";
        //return Table();
    }
    char buf[4096] = {0};

    // read ecsv statement
    int file_count = 0;
    infile.getline(buf, sizeof(buf));
    file_count++;
    if (regex_match(buf, regex("# %ECSV.*"))){
        if (!regex_match(buf, regex("# %ECSV (0\\.[1-9]*9|[1-9]+[0-9]*\\.[0-9]*)"))){
            cout << "[Warning] version of ECSV is less than 0.9\n";
        }
    }else{
        cout << "[Error] not ESCV format data file.\n";
        //return Table();
    }
    infile.getline(buf, sizeof(buf));
    file_count++;

    // read yaml header
    string yaml_part;
    while (infile.getline(buf, sizeof(buf))){
        file_count++;
        if (regex_match(buf, regex("# .*"))){
            if (!regex_match(buf, regex("##.*"))) yaml_part.append(buf+2).append("\n");
            else continue;
        }else break;
    }


    YAML::Node config = YAML::Load(yaml_part);
    // set meta and column
    Table table = Table(config);
    string delimiter = config["delimiter"].as<string>();
    string schema =config["schema"].as<string>();

    // read csv data
    file_count++;
    string tmp = buf;
    table.add_name_index(tmp, delimiter);

    int data_row_count = 0;
    int pos = infile.tellg();
    while (!infile.eof()){
        infile.getline(buf, sizeof(buf));
        // cout << buf << endl;
        data_row_count++;
    }
    infile.clear();
    infile.seekg(pos*sizeof(char), infile.beg); // back
    //table.test();
    table.def_data_shape(data_row_count);
    while (!infile.eof()){
        infile.getline(buf, sizeof(buf));
        if (strcmp(buf, "") == 0) continue;
        string tmp = buf;
        table.add_data(tmp, delimiter);
    }

    // end
    infile.close();
    return table;
};

void Transformer::table_to_FITS(){

};
void Transformer::table_to_csv(){

};
