# Misty
[![CircleCI](https://circleci.com/gh/rentziass/misty/tree/master.svg?style=shield)](https://circleci.com/gh/rentziass/misty/tree/master)
[![Go Report Card](https://goreportcard.com/badge/gojp/goreportcard)](https://goreportcard.com/report/rentziass/misty) 
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](/LICENSE) 

This project was inspired by [pgdump-obfuscator](https://github.com/ostrovok-team/pgdump-obfuscator).

This project needs a plain-text formatted dump to work, if the dump is
compressed you can create a plain-text version of it by using `pg_restore`:
```
pg_restore your_dump > dump.sql
```

To work with `misty` you need an `io.Reader` (eg: a dump file),
an `io.Writer` (could be `os.Stdout`) and a list
of targets you want to obfuscate, such as:
```golang
target := &misty.Target{
    TableName: "public.users",
    Columns: []*misty.TargetColumn{
        {
            Name:  "username",
            Value: func() []byte {
                return []byte("mistery_user")
            },
        },
    },
}
```

The target above will set `username` value in all the records of
the `public.users` table to be "mistery_user". As you can see column values
accept any function returning a slice of bytes, so you can get as fancy
as you like with those obfuscators.

You can play around with the [example](/example/main.go), providing a plain-text dump
named `dump.sql`.
