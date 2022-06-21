.PHONY: build
build: configure
	cmake --build build

.PHONY: configure
configure: 
	rm -rf build
	cmake -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true
	mv build/compile_commands.json ./

.PHONY: clean
clean:
	rm -rf build
