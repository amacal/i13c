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

Strace footprint:

```
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 12.19    0.000032           8         4           write
  8.01    0.000021           7         3           mmap
  9.56    0.000025           8         3           munmap
  4.15    0.000011          10         1           open
  2.70    0.000007           7         1           close
  2.44    0.000006           6         1           fstat
  4.37    0.000011          11         1           pread64
 56.58    0.000149         148         1           execve
------ ----------- ----------- --------- --------- ----------------
100.00    0.000263          17        15           total
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

Strace footprint:

```
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
  8.23    0.000035          11         3           mmap
  7.27    0.000031          10         3           munmap
  3.41    0.000015          14         1           write
  8.81    0.000037          37         1           open
  1.75    0.000007           7         1           close
  1.96    0.000008           8         1           fstat
  3.12    0.000013          13         1           pread64
 65.47    0.000278         278         1           execve
------ ----------- ----------- --------- --------- ----------------
100.00    0.000425          35        12           total
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

Strace footprint:

```
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
  3.08    0.000008           8         1           write
  4.44    0.000012          11         1           open
  3.41    0.000009           8         1           close
  2.62    0.000007           6         1           fstat
  2.99    0.000008           7         1           mmap
  3.48    0.000009           9         1           munmap
  4.44    0.000012          11         1           pread64
 75.54    0.000199         198         1           execve
------ ----------- ----------- --------- --------- ----------------
100.00    0.000263          32         8           total
```

## development

Everything is wired through the Makefile. The devcontainer provides all tooling, so you can just:

```bash
# builds all binaries
make build

# runs all unit tests
make test

# run all integration test
make integration

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
