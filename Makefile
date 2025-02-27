# Compiler and Flags
CXX = g++
CXXFLAGS = -Wall -std=c++17 -pthread -I C:/msys64/mingw64/include -I C:/vcpkg/installed/x64-windows/include

# Source Files
SRC = main.cpp hash_map.cpp fnv_hash.cpp persistence.cpp server.cpp
OBJ = $(SRC:.cpp=.o)

# Output Binary
TARGET = hash_map.exe  # Ensure .exe extension for Windows

# Build Rule
all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJ) -lpthread -lws2_32 -lmswsock  # Link Windows socket extensions

# Compile Each .cpp File into .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean Build Files
clean:
	@echo Cleaning up...
	rm -f $(OBJ) $(TARGET)  # Works for both MSYS2 and Linux
