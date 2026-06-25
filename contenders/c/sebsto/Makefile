### C contender for the demo-s3-archiving benchmark — AWS CRT edition.
###
### Builds a `bootstrap` binary linked against the AWS Common Runtime libraries
### (aws-c-s3 etc.). Expects the CRT install prefix to be in $CRT_PREFIX
### (set by the CodeBuild buildspec; defaults to /opt/aws-crt for local builds).

CRT_PREFIX ?= /opt/aws-crt
CC      ?= gcc
CFLAGS  ?= -O3 -march=armv8.2-a -flto -fno-plt -pipe -Wall -Wextra -Wno-unused-parameter -D_GNU_SOURCE -I$(CRT_PREFIX)/include
LDFLAGS ?= -O3 -flto -L$(CRT_PREFIX)/lib -L$(CRT_PREFIX)/lib64 -Wl,-rpath,'$$ORIGIN/lib'
# Static-link the CRT libraries; libcrypto + libc + system libs stay dynamic
# (AL2023 Lambda runtime ships libcrypto.so so this is safe).
LDLIBS   = \
    -Wl,-Bstatic \
    -laws-c-s3 -laws-c-auth -laws-c-http -laws-c-compression \
    -laws-c-sdkutils -laws-c-io -laws-c-cal -laws-checksums \
    -laws-c-common -ls2n \
    -Wl,-Bdynamic \
    -lcrypto -lz -lpthread -ldl -lm -lrt

SRC     := $(wildcard src/*.c)
OBJ     := $(SRC:.c=.o)
TARGET  := bootstrap

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJ) $(TARGET)
