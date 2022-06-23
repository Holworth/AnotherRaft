.PHONY: build
build: clean configure
	cmake -B build -DENABLE_RAFT_LOG=false
	cmake --build build

.PHONY: configure
configure: 
	rm -rf build
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true
	mv build/compile_commands.json ./

.PHONY: clean
clean:
	rm -rf build

.PHONY: log
log:
	cmake -B build -DENABLE_RAFT_LOG=true
	cmake --build build
