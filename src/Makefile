O			:= ../obj
CC			:= g++
CFLAGS		:= -std=c++11 -I/usr/include/mysql -L/usr/lib/mysql -lmysqlclient

SYS_DIR 	:= data_transform_system

OBJS = \
		$(O)/table.o \
		$(O)/transformer.o \
		$(O)/data_management_system.o \
		$(O)/data_transform_system.o \
		$(O)/main

.PHONY: obj build run clean

obj: main.cpp data_management_system.cpp data_transform_system.cpp $(SYS_DIR)/table.cpp $(SYS_DIR)/transformer.cpp 
	@$(CC) -c $(SYS_DIR)/table.cpp -o $(O)/table.o $(CFLAGS)
	@$(CC) -c $(SYS_DIR)/transformer.cpp -o $(O)/transformer.o $(CFLAGS)
	@$(CC) -c data_management_system.cpp  -o $(O)/data_management_system.o $(CFLAGS)
	@$(CC) -c data_transform_system.cpp -o $(O)/data_transform_system.o $(CFLAGS)
	@$(CC) -c main.cpp -o $(O)/main.cpp $(O)/main.o $(CFLAGS)

build: obj 
	@$(CC) $(OBJS) -o $(O)/main $(CFLAGS)

run: build 
	@./$(O)/main 

clean: 
	@rm -rf $(O)/*.o 
	@rm -rf $(O)/main