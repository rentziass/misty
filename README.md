# Misty
PostgreSQL database dump obfuscator

![](https://media.giphy.com/media/oAbvMXvah1M0U/giphy.gif)

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
of `misty.Target` you want to obfuscate.

## Altering column values
```golang
target := &misty.Target{
    TableName: "public.users",
    Columns: []*misty.TargetColumn{
        {
            Name:  "username",
            Value: func(_ []byte) []byte {
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

### Using existing column value
In case you want to do something with the existing column value (reusing,
condition something else, etc.), this is is passed in form of a `[]byte` as the
argument of your `TargetColumn` functions.

```golang
target := &misty.Target{
    TableName: "public.users",
    Columns: []*misty.TargetColumn{
        {
            Name:  "username",
            Value: func(oldVal []byte) []byte {
              if string(oldVal) == "rentziass" {
                return []("maintainer")
              }
              return []byte("mistery_user")
            },
        },
    },
}
```
This example will turn all the usernames to `mistery_user`, unless the real
username was `rentziass`, in which case it'll be changed to `maintainer`.

### Incremental values
If you need to have incremental values, to maintain uniqueness for example, you
can achieve that creating counters that are then used in your functions:

```golang
usernameCounter := 0
target := &misty.Target{
    TableName: "public.users",
    Columns: []*misty.TargetColumn{
        {
            Name:  "username",
            Value: func(oldVal []byte) []byte {
              usernameCounter++
              return []byte(fmt.Sprintf("user_%v", usernameCounter))
            },
        },
    },
}
```

These functions are called once for every single record in the target table, so
if three users were found in `public.users`, their usernames would be `user_1`,
`user_2` and `user_3`.


You can play around with the [example](/example/main.go), providing a plain-text dump
named `dump.sql`.

## Deleting rows
You can delete rows from your original dump by adding `DeleteRule`s to your
targets, like so:

```golang
target := &misty.Target{
    TableName: "public.users",
    DeleteRowRules: []*misty.DeleteRule{
        {
            ColumnName: "username",
            ShouldDelete: func(u []byte) bool {
              username := string(u)
              return string.HasPrefix(username, "admin")
            },
        },
    },
}
```

This will delete all the rows from `public.users` table where `username` begins
with "admin". You can have multiple conditions for the same table.

If you want to delete all the rows from a table you can achieve that with
something like

```golang
target := &misty.Target{
    TableName: "public.users",
    DeleteRowRules: []*misty.DeleteRule{
        {
            ColumnName: "id",
            ShouldDelete: func(_ []byte) bool {
              return true
            },
        },
    },
}
```
> Just make sure the column name you're using actually exists or you'll get an
> error.
