# i13c

A collection of ultra-lightweight Linux x86_64 utilities written in pure C. No external dependencies, no libc required. These tools are built for speed, transparency, and raw system introspection.

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
