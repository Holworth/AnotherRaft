.PHONY: release
build: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Release -DDEBUG=false
	cmake --build build

.PHONY: build
build: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DDEBUG=false
	mv build/compile_commands.json ./
	cmake --build build

.PHONY: debug
build: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DDEBUG=true
	mv build/compile_commands.json ./
	cmake --build build

.PHONY: clean
clean:
	rm -rf build

.PHONY: format
format:
	clang-format -i raft/*.h raft/*.cc
	clang-format -i kv/*.h kv/*.cc
	clang-format -i bench/*.h bench/*.cc
