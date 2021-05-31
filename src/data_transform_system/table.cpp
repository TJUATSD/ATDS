#include "Table.h"
// YAML::NodeType undefined 0 Null 1 Scalar 2 Sequence 3 Map 4

string Table::flatten(YAML::Node in){
    if (in.Type() == 3){
        string ret = "[";
        for (int i=0; i<in.size(); ++i){
            ret.append(flatten(in[i]));
            ret.append(",");
        }
        ret = ret.substr(0, ret.length()-1);
        ret.append("]");
        return ret;
    }else if (in.Type() == 4){
        string ret = "{";
        for (YAML::const_iterator it=in.begin(); it!=in.end(); ++it){
            ret.append(it->first.as<string>());
            ret.append(":");
            ret.append(flatten(it->second));
            ret.append(",");
        }
        ret = ret.substr(0, ret.length()-1);
        ret.append("}");
    }else if (in.IsScalar()){
        return in.as<string>();
    }else{
        cout << "[Warning] When flattening, the type of YAML::Node is undefined or NULL\n";
        return "";
    }
}


Table::Table(const Table& p){
    this->flag = p.flag;
    if (this->flag == 1){
        this->col_len = p.col_len;
        this->meta_len = p.meta_len;
        this->row_len = p.row_len;
        this->colname_index = p.colname_index;
        this->name_index = p.name_index;
        this->index_name = p.index_name;
        this->col_type = p.col_type;


        this->meta = (meta_data*)malloc((p.meta_len)*sizeof(meta_data));
        for (int i=0; i<p.meta_len; ++i){
            this->meta[i] = p.meta[i];
        }
        //cout << p.col_len;
        this->col_desc = (col*)malloc((p.col_len)*sizeof(col));
        for (int i=0; i<p.col_len; ++i){
            this->col_desc[i] = p.col_desc[i];
        }

        this->data = (void**)malloc(this->col_len * sizeof(void*));
        for (int i=0; i < this->col_len; ++i){
            map<int, string>::iterator it1 = this->index_name.find(i);
            string name = it1->second;
            map<string, int>::iterator it2 = this->colname_index.find(name);
            string datatype = this->col_desc[it2->second].datatype;
            if (datatype == "string"){
                this->data[i] = (char**)malloc(this->row_len*sizeof(char*));
                for (int j=0; j<this->row_len; ++j){
                    ((char**)this->data[i])[j] = (char*)malloc(max_string_column_element_size * sizeof(char));
                    strcpy((char*)((char**)this->data[i])[j], (char*)((char**)p.data[i])[j]);
                }
            }else if (datatype == "float64"){
                this->data[i] = (double*)malloc(this->row_len*sizeof(double));
                for (int j=0; j<this->row_len; ++j){
                    ((double*)this->data[i])[j] = ((double*)p.data[i])[j];
                }
            }else if (datatype == "float32"){
                this->data[i] = (float*)malloc(this->row_len * sizeof(float));
                for (int j=0; j<this->row_len; ++j){
                    ((float*)this->data[i])[j] = ((float*)p.data[i])[j];
                }
            }else if (datatype == "int16"){
                this->data[i] = (short*)malloc(this->row_len * sizeof(short));
                for (int j=0; j<this->row_len; ++j){
                    ((short*)this->data[i])[j] = ((short*)p.data[i])[j];
                }
            }else if (datatype == "int32"){
                this->data[i] = (int*)malloc(this->row_len * sizeof(int));
                for (int j=0; j<this->row_len; ++j){
                    ((int*)this->data[i])[j] = ((int*)p.data[i])[j];
                }
            }else{
                cout << "[Error] In Copy constructor, datatype isn't in any of defined type.\n";
            }
        }
    }
}


Table& Table::operator=(const Table& p){
    this->flag = p.flag;
    if (this->flag == 1){
        this->col_len = p.col_len;
        this->meta_len = p.meta_len;
        this->row_len = p.row_len;
        this->colname_index = p.colname_index;
        this->name_index = p.name_index;
        this->index_name = p.index_name;
        this->col_type = p.col_type;


        this->meta = (meta_data*)malloc((p.meta_len)*sizeof(meta_data));
        for (int i=0; i<p.meta_len; ++i){
            this->meta[i] = p.meta[i];
        }

        this->col_desc = (col*)malloc((p.col_len)*sizeof(col));
        for (int i=0; i<p.col_len; ++i){
            this->col_desc[i] = p.col_desc[i];
        }

        this->data = (void**)malloc(this->col_len * sizeof(void*));
        for (int i=0; i < this->col_len; ++i){
            map<int, string>::iterator it1 = this->index_name.find(i);
            string name = it1->second;
            map<string, int>::iterator it2 = this->colname_index.find(name);
            string datatype = this->col_desc[it2->second].datatype;
            if (datatype == "string"){
                this->data[i] = (char**)malloc(this->row_len*sizeof(char*));
                for (int j=0; j<this->row_len; ++j){
                    ((char**)this->data[i])[j] = (char*)malloc(max_string_column_element_size * sizeof(char));
                    strcpy((char*)((char**)this->data[i])[j], (char*)((char**)p.data[i])[j]);
                }
            }else if (datatype == "float64"){
                this->data[i] = (double*)malloc(this->row_len*sizeof(double));
                for (int j=0; j<this->row_len; ++j){
                    ((double*)this->data[i])[j] = ((double*)p.data[i])[j];
                }
            }else if (datatype == "float32"){
                this->data[i] = (float*)malloc(this->row_len * sizeof(float));
                for (int j=0; j<this->row_len; ++j){
                    ((float*)this->data[i])[j] = ((float*)p.data[i])[j];
                }
            }else if (datatype == "int16"){
                this->data[i] = (short*)malloc(this->row_len * sizeof(short));
                for (int j=0; j<this->row_len; ++j){
                    ((short*)this->data[i])[j] = ((short*)p.data[i])[j];
                }
            }else if (datatype == "int32"){
                this->data[i] = (int*)malloc(this->row_len * sizeof(int));
                for (int j=0; j<this->row_len; ++j){
                    ((int*)this->data[i])[j] = ((int*)p.data[i])[j];
                }
            }else{
                cout << "[Error] In Assignment constructor, datatype isn't in any of defined type.\n";
            }
        }
    }
    return *this;
}


void Table::serialization(meta_data meta){
    cout << "key: " << string(meta.key) << ", value: " << string(meta.value) << "\n";
}

void Table::serialization(col column){
    cout << "name: " << string(column.name) << ", unit: " << string(column.unit) << ", datatype: " << string(column.datatype) << "\n";
}

void Table::assign(meta_data &meta, string _key, string _value){
    meta.key = (char*)malloc(_key.length()*sizeof(char)+2);
    meta.value = (char*)malloc(_value.length()*sizeof(char)+2);
    strcpy(meta.key, _key.c_str());
    strcpy(meta.value, _value.c_str());
}

void Table::assign(col &column, string _name, string _unit, string _datatype){
    column.name = (char*)malloc(_name.length()*sizeof(char)+2);
    column.unit = (char*)malloc(_unit.length()*sizeof(char)+2);
    column.datatype = (char*)malloc(_datatype.length()*sizeof(char)+2);
    strcpy(column.name, _name.c_str());
    strcpy(column.unit, _unit.c_str());
    strcpy(column.datatype, _datatype.c_str());
}

void Table::add_name_index(string t, string delimiter){
    // correspond name and order of column
    int pos1 = 0, pos2 = 0, i = 0;
    while ((pos2 = t.find(delimiter, pos1)) != -1){
        this->name_index.insert(pair<string, int>(t.substr(pos1, pos2-pos1), i));
        this->index_name.insert(pair<int, string>(i, t.substr(pos1, pos2-pos1)));
        pos1 = pos2+1;
        i += 1;
    }
    this->name_index.insert(pair<string, int>(t.substr(pos1, t.length()-pos1), i));
    this->index_name.insert(pair<int, string>(i, t.substr(pos1, t.length()-pos1)));
}

void Table::def_data_shape(int _row_len){
    this->data = (void**)malloc(this->col_len * sizeof(void*));
    for (int i=0; i < this->col_len; ++i){
        map<int, string>::iterator it1 = this->index_name.find(i);
        string name = it1->second;
        map<string, int>::iterator it2 = this->colname_index.find(name);
        string datatype = this->col_desc[it2->second].datatype;
        if (datatype == "string"){
            this->data[i] = (char**)malloc(_row_len*sizeof(char*));
            for (int j=0; j<_row_len; ++j){
                ((char**)this->data[i])[j] = (char*)malloc(max_string_column_element_size * sizeof(char));
            }
            this->col_type.insert(pair<int, int>(i, 0));
        }else if (datatype == "float64"){
            this->data[i] = (double*)malloc(_row_len*sizeof(double));
            this->col_type.insert(pair<int, int>(i, 1));
        }else if (datatype == "float32"){
            this->data[i] = (float*)malloc(_row_len * sizeof(float));
            this->col_type.insert(pair<int, int>(i, 2));
        }else if (datatype == "int16"){
            this->data[i] = (short*)malloc(_row_len * sizeof(short));
            this->col_type.insert(pair<int, int>(i, 3));
        }else if (datatype == "int32"){
            this->data[i] = (int*)malloc(_row_len * sizeof(int));
            this->col_type.insert(pair<int, int>(i, 4));
        }else{
            cout << "[Error] When def_data_shape(), datatype isn't in any of defined type.\n";
        }
    }
    this->row_len = 0;
}

void Table::add_data(string t, string delimiter){
    // assign substring, separated by 'delimiter',  into data array
    int pos1 = 0, pos2 = 0, i = 0, value;
    string t1;
    map<int, int>::iterator it1;
    while ((pos2 = t.find(delimiter, pos1)) != -1){
        t1 = t.substr(pos1, pos2-pos1);
        it1 = this->col_type.find(i);
        value = it1->second;
        if (value == 0){
            strcpy((char*)((char**)this->data[i])[this->row_len], t1.c_str());
        }else if (value == 1){
            ((double*)this->data[i])[this->row_len] = stod(t1);
        }else if (value == 2){
            ((float*)this->data[i])[this->row_len] = stof(t1);
        }else if (value == 3){
            ((short*)this->data[i])[this->row_len] = (short)stoi(t1);
        }else if (value == 4){
            ((int*)this->data[i])[this->row_len] = stoi(t1);
        }else{
            cout << "[Error] When add_data(), value is unknown.\n";
        }
        pos1 = pos2+1;
        i += 1;
    }
    t1 = t.substr(pos1, t.length()-pos1);
    it1 = this->col_type.find(i);
    value = it1->second;
    if (value == 0){
        strcpy(((char**)this->data[i])[this->row_len], t1.c_str());
    }else if (value == 1){
        ((double*)this->data[i])[this->row_len] = stod(t1);
    }else if (value == 2){
        ((float*)this->data[i])[this->row_len] = stof(t1);
    }else if (value == 3){
        ((short*)this->data[i])[this->row_len] = (short)stoi(t1);
    }else if (value == 4){
        ((int*)this->data[i])[this->row_len] = stoi(t1);
    }else{
        cout << "[Error] When add_data(), value is unknown.\n";
    }
    this->row_len += 1;
}

Table::Table(YAML::Node config){
    // cout << "Table(YAML::Node config)\n";
    // flag to mark constructor
    this->flag = 1;
    // input datatype and meta
    // input meta
    YAML::Node meta = config["meta"];
    if (meta.Type() == 3){
        this->meta_len = 0;
        this->meta = (meta_data*)malloc((meta.size())*sizeof(meta_data));
        for (int i=0; i<meta.size(); ++i){
            for (YAML::const_iterator it=meta[i].begin(); it!=meta[i].end(); ++it){
                string key = it->first.as<string>();
                //cout << key << endl;
                if (it->second.IsScalar()){
                    Table::assign(this->meta[this->meta_len], key, it->second.as<string>());
                    this->meta_len += 1;
                }else{
                    string value = flatten(it->second);
                    assign(this->meta[this->meta_len], key, value);
                    this->meta_len += 1;
                }
            }
        }
    }else if (meta.Type() == 4){
        cout << "[Error] meta part is dictionary\n";
    }else{
        cout << "[Error] meta part is unrecognizable\n";
    }
    YAML::Node column = config["datatype"];
    if (column.Type() == 3){
        this->col_len = 0;
        this->col_desc = (col*)malloc(column.size()*sizeof(col));
        for (int i=0; i<column.size(); ++i){
            string name = column[i]["name"].as<string>();
            string unit = "";
            if (column[i]["unit"].Type() == 2){
                unit = column[i]["unit"].as<string>();
            }
            string datatype = column[i]["datatype"].as<string>();
            this->colname_index.insert(pair<string, int>(name, i));
            Table::assign(this->col_desc[this->col_len], name, unit, datatype);
            this->col_len += 1;
        }
    }else if (meta.Type() == 4){
        cout << "[Error] column part is dictionary\n";
    }else{
        cout << "[Error] column part is unrecognizable\n";
    }

}

Table::Table(){
    // cout << "Table()\n";
    this->flag = 0;
}

Table::~Table(){

    if (flag == 1){
        for (int i=0; i<this->meta_len; ++i){
            free(this->meta[i].key);
            free(this->meta[i].value);
        }

        free(this->meta);

        for (int i=0; i<this->col_len; ++i){
            free(this->col_desc[i].name);
            free(this->col_desc[i].unit);
            free(this->col_desc[i].datatype);
        }

        free(this->col_desc);

        for (int i=0; i < this->col_len; i++){
            // 0��string��1��double��2��float��3��short��4��int
            map<int, int>::iterator it = this->col_type.find(i);
            if (it->second == 0){
                for (int j=0; j<this->row_len; j++){
                    free(((char**)this->data[i])[j]);
                }
                free(this->data[i]);
            }else if (it->second == 1){
                free((double*)this->data[i]);
            }else if (it->second == 2){
                free((float*)this->data[i]);
            }else if (it->second == 3){
                free((short*)this->data[i]);
            }else if (it->second == 4){
                free((int*)this->data[i]);
            }
        }
        free((void*)this->data);
    }

}

string Table::find_meta_value (string _key) const{
    for (int i=0; i < this->meta_len; ++i){
        if (strcmp(this->meta[i].key, _key.c_str()) == 0){
            return string(this->meta[i].value);
        }
    }
    return string("");
}

void Table::test(){
    for (map<int, string>::iterator it=this->index_name.begin();it != this->index_name.end(); ++it){
        cout << "key: " << it->first << " value: " << it->second << endl;
    }
    for (map<string, int>::iterator it=this->name_index.begin();it != this->name_index.end(); ++it){
        cout << "key: " << it->first << " value: " << it->second << endl;
    }
}
