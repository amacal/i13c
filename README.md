# i13c

A collection of ultra-lightweight Linux x86/64 utilities written in pure C. No external dependencies, no libc required. These tools are built for fun, speed, transparency and raw system introspection.

## tools

### **i13c-thrift**

A minimal parser and dumper for Thrift Compact Protocol.

#### Reads binary-encoded Thrift data from standard input and prints a human-readable field structure

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

A minimal parser and dumper for parquet files.

#### Shows metadata section from the parquet files in a human readable form

```bash
i13c-parquet show-metadata data/test01.parquet | head -n 25
```

Example output:

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

#### Shows schema of the parquet file in a spark-like form

```bash
i13c-parquet show-schema data/test04.parquet
```

Example output:

```
duckdb_schema, REQUIRED
 |-- PassengerId, INT64, INT64, OPTIONAL
 |-- Survived, INT64, INT64, OPTIONAL
 |-- Pclass, INT64, INT64, OPTIONAL
 |-- Name, UTF8, BYTE_ARRAY, OPTIONAL
 |-- Sex, UTF8, BYTE_ARRAY, OPTIONAL
 |-- Age, DOUBLE, OPTIONAL
 |-- SibSp, INT64, INT64, OPTIONAL
 |-- Parch, INT64, INT64, OPTIONAL
 |-- Ticket, UTF8, BYTE_ARRAY, OPTIONAL
 |-- Fare, DOUBLE, OPTIONAL
 |-- Cabin, UTF8, BYTE_ARRAY, OPTIONAL
 |-- Embarked, UTF8, BYTE_ARRAY, OPTIONAL
```

#### Extracts metadata section from the parquet files and streams it into stdout

```bash
i13c-parquet extract-metadata data/test01.parquet | i13c-thrift | head -n 25
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
    index=1, type=struct
     struct-start
      field=1, type=i32, value=1
      field=3, type=i32, value=1
      field=4, type=binary, size=4, ascii=date
      field=6, type=i32, value=6
      field=9, type=i32, value=1
      field=10, type=struct
       struct-start
        field=6, type=struct
         struct-start
          field=0, type=stop
         struct-end
        field=0, type=stop
       struct-end
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
make parquet ARGS="command file.parquet"
```

# license

This project is licensed under the [PolyForm Noncommercial License 1.0.0](LICENSE).
