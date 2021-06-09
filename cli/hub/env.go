package main

import (
	"os"
	"strconv"
)

func getAddr() string {
	if addr, ok := os.LookupEnv("ADDR"); ok {
		return addr
	}

	return "127.0.0.1"
}

func getPort() uint64 {
	if port, ok := os.LookupEnv("PORT"); ok {
		if parsed, err := strconv.ParseUint(port, 10, 64); err == nil {
			return parsed
		}
	}

	return 13000
}

func getCapacity() uint64 {
	if _cap, ok := os.LookupEnv("CAPACITY"); ok {
		if parsed, err := strconv.ParseUint(_cap, 10, 64); err == nil {
			return parsed
		}
	}

	return 4096
}
