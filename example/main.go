package main

import (
	"log"
	"os"

	"github.com/icrowley/fake"

	"github.com/rentziass/misty"
)

func main() {
	f, err := os.Open("dump.sql")
	if err != nil {
		panic(err)
	}

	target := &misty.Target{
		TableName: "public.users",
		Columns: []*misty.TargetColumn{
			{
				Name:  "username",
				Value: obfuscateHandle,
			},
			{
				Name:  "email",
				Value: obfuscateEmail,
			},
		},
	}

	err = misty.Obfuscate(f, os.Stdout, []*misty.Target{target})
	if err != nil {
		log.Println(err)
	}

}

func obfuscateHandle() []byte {
	return []byte(fake.UserName())
}

func obfuscateEmail() []byte {
	return []byte(fake.EmailAddress())
}
