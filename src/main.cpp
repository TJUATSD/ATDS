#include <iostream>
#include "data_transform_system.h"
#include <string>
#include <io.h>
#include <direct.h>

using std::cout;
using std::cin;
using std::endl;

int main(int argc, char** argv) {
	int option = 0;
	while (option != 3){
		cout << "[info]press 1 to import data from dataset, press 2 to export data from database and press 3 to exit.\n";

		scanf("%d", &option);
		if (option == 1){
			// import data from dataset
			int option1 = 0;
			while (option1 != 1){
				cout << "[info]input the directory of dataset. the directory name should be less than 1000 charactors.\n";
				cout << "[info]or press 0 to exit.\n";
				char dir[1000];
				scanf("%s", dir);
				//cout << dir;

				if (strcmp(dir, "0") == 0){
					cout << "[info]return to menu.\n";
					break;
				}else{
					if (access(dir, 0) == -1){
						cout << "[fail]target directory doesn't exist.\n";
						continue;
					}else{
						cout << "[ok]target directory found.\n";
						// target directory found
						Data_transform_system DTS = Data_transform_system();
						DTS.data_import(dir, strlen(dir));
						cout << "[Info] Import end.\n";
					}
				}
			}
		}else if (option == 2){
			// export data from database
			int option2 = 0;
			while (option2 != 1){
                cout << "[Info] Press 1 to output all time-serial data.\n";
                cout << "[Info] Press 2 to output all dataset.\n";
                cout << "[Info] Press 3 to exit.\n";
                int t;
                scanf("%d", &t);

                if (t == 1){
                    option2 = 1;
                    int option21 = 0;

                    while (option21 != 1){
                        cout << "[Info] Input the directory to export or press 0 to exit.\n";
                        char dir[1000];
                        scanf("%s", dir);

                        if (strcmp(dir, "0") == 0){
                            cout << "[info]return to menu.\n";
                            break;
                        }else if (access(dir, 0) == -1){
                            cout << "[fail]target directory doesn't exist.\n";
                            continue;
                        }else{
                            option21 = 1;
                            Data_transform_system DTS = Data_transform_system();
                            DTS.data_export(dir, strlen(dir));
                            // cout << "test\n";
                        }
                    }

                    cout << "[Info] Output all time-serial data done.\n";
                }else if (t == 2){
                    option2 = 1;
                    int option22 = 0;

                    while (option22 != 1){
                        cout << "[Info] Input the directory to export or press 0 to exit.\n";
                        char dir[1000];
                        scanf("%s", dir);

                        if (strcmp(dir, "0") == 0){
                            cout << "[info]return to menu.\n";
                            break;
                        }else if (access(dir, 0) == -1){
                            cout << "[fail]target directory doesn't exist.\n";
                            continue;
                        }else{
                            option22 = 1;
                            Data_transform_system DTS = Data_transform_system();
                            DTS.dataset_export(dir, strlen(dir));
                        }
                    }

                    cout << "[Info] Output all dataset done.\n";
                }else if (t == 3){
                    option2 = 1;
                    cout << "[Info] Exiting export data.\n";
                }else {
                    cout << "[Warning] Wrong input.\n";
                    continue;
                }
			}
		}else if (option == 3){
			cout << "[info]the service is done.\n";
		}else{
			cout << "[fail]wrong data input. follow the hint as follow.\n";
		}
	}
	return 0;
}
