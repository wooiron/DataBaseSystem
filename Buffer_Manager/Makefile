.SUFFIXES: .cpp .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/


# SRCS:=$(wildcard src/*.cpp)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cpp
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.cpp $(SRCDIR)file.cpp $(SRCDIR)buffer.cpp
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cpp=.o)

CPPFLAGS= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ)
	$(foreach file, $(SRCS_FOR_LIB), $(CC) $(CPPFLAGS) -o $(file:.cpp=.o) -c $(file);)
	make static_library
	$(CC) $(CXXFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	g++ -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
