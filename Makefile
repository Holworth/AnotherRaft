.PHONY: release
release: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=off
	mv build/compile_commands.json ./
	cmake --build build

.PHONY: build
build: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=off
	mv build/compile_commands.json ./
	cmake --build build

.PHONY: log
log: clean 
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=on
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

