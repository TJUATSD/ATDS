#include "data_transform_system.h"
#define MAX_THREADS 1
#define THRESHOLD 100
/*
	Name: astro_data_system
	Copyright: BSD
	Author: myf
	Date: 16/03/21 11:24
	Description: A system handles astro_data
*/

Data_transform_system::Data_transform_system(){
	// cout << "[info]no directory is defined yet\n";
	// throw "no directory is defined\n";
}

Data_transform_system::Data_transform_system(char dir[]){
	if (access(dir, 0) == -1){
		cout << "[fail]target directory doesn't exist.\n";
	}else{
		strcpy(this->dir, dir);
		cout << "[info] get the target directory of: "<< this->dir << endl;
	}
}

Data_transform_system::~Data_transform_system(){

}

void Data_transform_system::init(){

}


void Data_transform_system::data_export(char dir[], int size){
    // cout << "[info] Diving into the data_transform_system now\n";
	if (access(dir, 0) == -1){
		// cout << "[fail]target directory doesn't exist.\n";
	}else{
		strncpy(this->dir, dir, 1024);
		// cout << "[info] get the target directory of: "<< this->dir << endl;

		// ��ʱ
        time_t start,end;
        start =time(NULL);//or time(&start);
        //��calculating��

		Data_management_system dataMS = Data_management_system();
		dataMS.data_export(dir, strlen(dir));

		end =time(NULL);
        printf("[Debug] All time��%ld\n",end - start);

		cout << "[Info] data_export done.\n";
	}
};

void Data_transform_system::dataset_export(char dir[], int size){
    // cout << "[info] Diving into the data_transform_system now\n";
	if (access(dir, 0) == -1){
		// cout << "[fail]target directory doesn't exist.\n";
	}else{
		strncpy(this->dir, dir, 1024);
		// cout << "[info] get the target directory of: "<< this->dir << endl;

		// ��ʱ
        time_t start,end;
        start =time(NULL);//or time(&start);
        //��calculating��

		Data_management_system dataMS = Data_management_system();
		dataMS.data_export2(dir, strlen(dir));

		end =time(NULL);
        printf("[Debug] All time��%ld\n",end - start);

        cout << "[Info] dataset_export done.\n";
	}
}

typedef struct thread_struct{
    int i;
    char dir[1024];
}thread_struct, *thread_struct_p;


void* thread( void *arg )
{
    //system("pause");
    int cur_pnum = (*(thread_struct*)arg).i;

    // Initialzation
    long long hFile = 0;
    struct _finddata_t fileinfo;

    string cur_p = (*(thread_struct*)arg).dir;
    Transformer trsf = Transformer();
    Data_management_system dataMS = Data_management_system();

    // [Debug] time test
    int count = 0;
    int threshold = THRESHOLD;

    // Iterate through the file
    // dirs.pop();
    if ((hFile = _findfirst((cur_p+"\\*").c_str(), &fileinfo)) != -1){
        // when existing the first file
        string cur_p = (*(thread_struct*)arg).dir;

        do{
            if ((fileinfo.attrib & _A_SUBDIR)){
                // ignoring the sub directory
                continue;
            }else{
                // read data file
                // cur_p = dirs.top();
                char dir_tmp[2048];
                string tmp = cur_p;
                tmp += "\\";
                tmp += fileinfo.name;
                if (access(tmp.c_str(), 0) == -1){
                    cout << "[Error] failed to open " << tmp << endl;
                }else if (tmp.length() >= 2048){
                    cout << tmp << "is longer than 2048\n";
                    continue;
                }else if (count % MAX_THREADS == cur_pnum){
                    strncpy(dir_tmp, tmp.c_str(), 2048);
                    Table tmp_table = trsf.csv_to_table(dir_tmp, strlen(dir_tmp));
                    dataMS.data_import(tmp_table);
                }
                ++count;

                if (count % (100*MAX_THREADS) == 0) cout << "[Debug] Finish " << count/MAX_THREADS << "files.\n";
            }
        }while (_findnext(hFile, &fileinfo) == 0 && count < threshold);
        _findclose(hFile);
    }
    cout << "[Debug] Finish all" << count/MAX_THREADS << "files.\n";
}

void Data_transform_system::data_import(char dir[], int size){

	if (access(dir, 0) == -1){
		cout << "[fail]target directory doesn't exist.\n";
	}else{
		strncpy(this->dir, dir, 1024);
		Transformer tsf = Transformer();

		// [Debug] Clear the database.
		Data_management_system dataMS1 = Data_management_system();
        dataMS1.clear_dataset();

		// [Debug] Time test.
        time_t start,end;
        start =time(NULL);//or time(&start);


        pthread_t th[MAX_THREADS];
        int ret;
        int *thread_ret = NULL;
        thread_struct num_array[MAX_THREADS];

        for (int i=0; i<MAX_THREADS; i++){
            num_array[i].i = i;
            strcpy(num_array[i].dir,this->dir);
        }

        // run it with multi-thread
        for( int i=0; i<MAX_THREADS; i++ ){
            ret = pthread_create( &th[i], NULL, thread, &num_array[i] );
            if( ret != 0 ){
                printf( "Create thread error!\n");
                ExitProcess(3);
            }
        }

		// wait and join
		for (int i=0; i<MAX_THREADS; i++){
            pthread_join(th[i], (void**)thread_ret);
        }

		// [Debug] Time test done.
		end =time(NULL);
        printf("����ʱ��%ld\n",end - start);

	}
}

