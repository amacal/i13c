# i13c

A collection of ultra-lightweight Linux x86/64 utilities written in pure C. No external dependencies, no libc required. These tools are built for fun, speed, transparency and raw system introspection.

## tools

### **i13c-thrift**

A minimal parser and dumper for Thrift Compact Protocol. Reads binary-encoded Thrift data from standard input and prints a human-readable field structure.

```bash
cat data.thrift | i13c-thrift
```

Example output:

```
struct-start
 field=1, type=i32, value=1
 field=2, type=list, size=6, item-type=struct
  list-start
   index=0, type=struct
    struct-start
     field=4, type=binary, size=5, ascii=table
     field=5, type=i32, value=5
     field=0, type=stop
    struct-end
  list-end
struct-end
```

Supports: primitive types, nested structs, lists, zigzag decoding.

### **i13c-parquet**

A minimal parser and dumper for parquet files. Reads metadata section from the file footer and prints a human-readable field structure.

```bash
i13c-parquet data/test01.parquet
```

Example output (truncated):

```
struct-start, type=metadata
 version, type=i32
  1
 schemas, type=struct
  array-start
   index-start, index=0, type=struct
    struct-start, type=schema_element
     name, type=text
      table
     num_children, type=i32
      5
    struct-end
   index-end
   index-start, index=1, type=struct
    struct-start, type=schema_element
     data_type, type=enum
      INT32
     repetition_type, type=enum
      OPTIONAL
     name, type=text
      date
     converted_type, type=enum
      DATE
    struct-end
   index-end
```

## development

Everything is wired through the Makefile. The devcontainer provides all tooling, so you can just:

```bash
# builds all binaries
make build

# runs all tests
make test

# checks formatting
make lint

# reformats sources
make fix

# runs i13c-thrift
make thrift

# runs i13c-parquet
make parquet ARGS="file.parquet"
```

# license

This project is licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License](LICENSE).
