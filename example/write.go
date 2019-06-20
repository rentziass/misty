package main

import "os"

func main() {
	f, err := os.OpenFile("writing.test", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	_, err = f.WriteAt([]byte("some"), 100)
	if err != nil {
		panic(err)
	}
	f.Close()
}
