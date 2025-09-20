#include "parquet.iter.h"
#include "dom.h"
#include "malloc.h"
#include "parquet.base.h"
#include "runner.h"
#include "stdout.h"
#include "typing.h"

static const char *const PARQUET_COMPRESSION_NAMES[PARQUET_COMPRESSION_SIZE] = {
  [PARQUET_COMPRESSION_UNCOMPRESSED] = "UNCOMPRESSED",
  [PARQUET_COMPRESSION_SNAPPY] = "SNAPPY",
  [PARQUET_COMPRESSION_GZIP] = "GZIP",
  [PARQUET_COMPRESSION_LZO] = "LZO",
  [PARQUET_COMPRESSION_BROTLI] = "BROTLI",
  [PARQUET_COMPRESSION_LZ4] = "LZ4",
  [PARQUET_COMPRESSION_ZSTD] = "ZSTD",
  [PARQUET_COMPRESSION_LZ4_RAW] = "LZ4_RAW",
};

static const char *const PARQUET_CONVERTED_TYPE_NAMES[PARQUET_CONVERTED_TYPE_SIZE] = {
  [PARQUET_CONVERTED_TYPE_UTF8] = "UTF8",
  [PARQUET_CONVERTED_TYPE_MAP] = "MAP",
  [PARQUET_CONVERTED_TYPE_MAP_KEY_VALUE] = "MAP_KEY_VALUE",
  [PARQUET_CONVERTED_TYPE_LIST] = "LIST",
  [PARQUET_CONVERTED_TYPE_ENUM] = "ENUM",
  [PARQUET_CONVERTED_TYPE_DECIMAL] = "DECIMAL",
  [PARQUET_CONVERTED_TYPE_DATE] = "DATE",
  [PARQUET_CONVERTED_TYPE_TIME_MILLIS] = "TIME_MILLIS",
  [PARQUET_CONVERTED_TYPE_TIME_MICROS] = "TIME_MICROS",
  [PARQUET_CONVERTED_TYPE_TIMESTAMP_MILLIS] = "TIMESTAMP_MILLIS",
  [PARQUET_CONVERTED_TYPE_TIMESTAMP_MICROS] = "TIMESTAMP_MICROS",
  [PARQUET_CONVERTED_TYPE_UINT8] = "UINT8",
  [PARQUET_CONVERTED_TYPE_UINT16] = "UINT16",
  [PARQUET_CONVERTED_TYPE_UINT32] = "UINT32",
  [PARQUET_CONVERTED_TYPE_UINT64] = "UINT64",
  [PARQUET_CONVERTED_TYPE_INT8] = "INT8",
  [PARQUET_CONVERTED_TYPE_INT16] = "INT16",
  [PARQUET_CONVERTED_TYPE_INT32] = "INT32",
  [PARQUET_CONVERTED_TYPE_INT64] = "INT64",
  [PARQUET_CONVERTED_TYPE_JSON] = "JSON",
  [PARQUET_CONVERTED_TYPE_BSON] = "BSON",
  [PARQUET_CONVERTED_TYPE_INTERVAL] = "INTERVAL",
};

static const char *const PARQUET_DATA_TYPE_NAMES[PARQUET_DATA_TYPE_SIZE] = {
  [PARQUET_DATA_TYPE_BOOLEAN] = "BOOLEAN",
  [PARQUET_DATA_TYPE_INT32] = "INT32",
  [PARQUET_DATA_TYPE_INT64] = "INT64",
  [PARQUET_DATA_TYPE_INT96] = "INT96",
  [PARQUET_DATA_TYPE_FLOAT] = "FLOAT",
  [PARQUET_DATA_TYPE_DOUBLE] = "DOUBLE",
  [PARQUET_DATA_TYPE_BYTE_ARRAY] = "BYTE_ARRAY",
  [PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED] = "PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED",
};

static const char *const PARQUET_ENCODING_NAMES[PARQUET_ENCODING_SIZE] = {
  [PARQUET_ENCODING_PLAIN] = "PLAIN",
  [PARQUET_ENCODING_GROUP_VAR_INT] = "GROUP_VAR_INT",
  [PARQUET_ENCODING_PLAIN_DICTIONARY] = "PLAIN_DICTIONARY",
  [PARQUET_ENCODING_RLE] = "RLE",
  [PARQUET_ENCODING_BIT_PACKED] = "BIT_PACKED",
  [PARQUET_ENCODING_DELTA_BINARY_PACKED] = "DELTA_BINARY_PACKED",
  [PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY] = "DELTA_LENGTH_BYTE_ARRAY",
  [PARQUET_ENCODING_DELTA_BYTE_ARRAY] = "DELTA_BYTE_ARRAY",
  [PARQUET_ENCODING_RLE_DICTIONARY] = "RLE_DICTIONARY",
  [PARQUET_ENCODING_BYTE_STREAM_SPLIT] = "BYTE_STREAM_SPLIT",
};

static const char *const PARQUET_PAGE_TYPE_NAMES[PARQUET_PAGE_TYPE_SIZE] = {
  [PARQUET_PAGE_TYPE_DATA_PAGE] = "DATA_PAGE",
  [PARQUET_PAGE_TYPE_INDEX_PAGE] = "INDEX_PAGE",
  [PARQUET_PAGE_TYPE_DICTIONARY_PAGE] = "DICTIONARY_PAGE",
  [PARQUET_PAGE_TYPE_DATA_PAGE_V2] = "DATA_PAGE_V2",
};

static const char *const PARQUET_REPETITION_TYPE_NAMES[PARQUET_REPETITION_TYPE_SIZE] = {
  [PARQUET_REPETITION_TYPE_REQUIRED] = "REQUIRED",
  [PARQUET_REPETITION_TYPE_OPTIONAL] = "OPTIONAL",
  [PARQUET_REPETITION_TYPE_REPEATED] = "REPEATED",
};

static i64
parquet_dump_enum(struct parquet_metadata_iterator *iterator, u32 index, i32 size, const char *const *names) {
  i32 *value;
  const char *name;

  // check for the capacity, we need only 1 slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // extract the value and the name
  name = NULL;
  value = (i32 *)iterator->queue.items[index].ctx;

  // find mapping if available
  if (*value < size) {
    name = names[*value];
  }

  // output either raw i32 or mapped name
  if (name == NULL) {
    iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
    iterator->tokens.items[iterator->tokens.count].data = (u64)*value;
    iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_I32;
  } else {
    iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
    iterator->tokens.items[iterator->tokens.count].data = (u64)name;
    iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;
  }

  // success
  return 0;
}

static i64 parquet_dump_compression_codec(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_COMPRESSION_SIZE, PARQUET_COMPRESSION_NAMES);
}

static i64 parquet_dump_converted_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_CONVERTED_TYPE_SIZE, PARQUET_CONVERTED_TYPE_NAMES);
}

static i64 parquet_dump_data_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);
}

static i64 parquet_dump_encoding(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_ENCODING_SIZE, PARQUET_ENCODING_NAMES);
}

static i64 parquet_dump_page_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_PAGE_TYPE_SIZE, PARQUET_PAGE_TYPE_NAMES);
}

static i64 parquet_dump_repetition_type(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_enum(iterator, index, PARQUET_REPETITION_TYPE_SIZE, PARQUET_REPETITION_TYPE_NAMES);
}

static i64 parquet_dump_literal(struct parquet_metadata_iterator *iterator, u32 index, u8 type) {
  u64 value;

  // check for the capacity, we need only 1 slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // extract the value
  switch (type) {
    case DOM_TYPE_I32:
      value = (u64) * (i32 *)iterator->queue.items[index].ctx;
      break;
    case DOM_TYPE_I64:
      value = (u64) * (i64 *)iterator->queue.items[index].ctx;
      break;
    case DOM_TYPE_TEXT:
      value = (u64)(const char *)iterator->queue.items[index].ctx;
      break;
  }

  // value literal
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = value;
  iterator->tokens.items[iterator->tokens.count++].type = type;

  // success
  return 0;
}

static i64 parquet_dump_i32(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_I32);
}

static i64 parquet_dump_i64(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_I64);
}

static i64 parquet_dump_text(struct parquet_metadata_iterator *iterator, u32 index) {
  return parquet_dump_literal(iterator, index, DOM_TYPE_TEXT);
}

static i64 parquet_dump_struct_open(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_STRUCT_START;
  iterator->tokens.items[iterator->tokens.count++].data = iterator->queue.items[index].ctx_args.value;

  return 0;
}

static i64 parquet_dump_struct_close(struct parquet_metadata_iterator *iterator, u32 index) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // struct end
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_STRUCT_END;
  iterator->tokens.items[iterator->tokens.count++].data = iterator->queue.items[index].ctx_args.value;

  return 0;
}

static i64 parquet_dump_array_open(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_ARRAY_START;
  iterator->tokens.items[iterator->tokens.count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_array_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // array end
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_ARRAY_END;
  iterator->tokens.items[iterator->tokens.count++].data = (u64)(i64)-1;

  return 0;
}

static i64 parquet_dump_value_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // value end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_VALUE_END;

  // success
  return 0;
}

static i64 parquet_dump_index_open(struct parquet_metadata_iterator *iterator, u32 index) {
  u64 ctx_args;

  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // get args
  ctx_args = iterator->queue.items[index].ctx_args.value;

  // index start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_INDEX_START;
  iterator->tokens.items[iterator->tokens.count++].data = ctx_args;

  // success
  return 0;
}

static i64 parquet_dump_index_close(struct parquet_metadata_iterator *iterator, u32) {
  // check for the capacity, we need only one slot
  if (iterator->tokens.count > iterator->tokens.capacity - 1) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // index end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_INDEX_END;

  // success
  return 0;
}

static i64 parquet_dump_field(struct parquet_metadata_iterator *iterator, u32 index) {
  i32 *ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // check for the capacity, we need 4 slots
  if (iterator->tokens.count > iterator->tokens.capacity - 4) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // check for the capacity, we need 2 slots
  if (iterator->queue.count > iterator->queue.capacity - 2) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // extract
  ctx = iterator->queue.items[index].ctx;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;
  item_fn = iterator->queue.items[index].item_fn;

  // key-start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_KEY_START;
  iterator->tokens.items[iterator->tokens.count].type = DOM_TYPE_TEXT;
  iterator->tokens.items[iterator->tokens.count++].data = (u64) "text";

  // key-content
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = ctx_args;
  iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;

  // key-end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_KEY_END;

  // value-start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_VALUE_START;
  iterator->tokens.items[iterator->tokens.count++].data = item_args;

  // value-close
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_value_close;

  // value-content
  iterator->queue.items[iterator->queue.count].ctx = ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = item_fn;

  // success
  return 0;
}

static i64 parquet_dump_index(struct parquet_metadata_iterator *iterator, u32 index) {
  void **ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // get the value
  ctx = (void **)iterator->queue.items[index].ctx;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;
  item_fn = iterator->queue.items[index].item_fn;

  // check for null-terminated
  if (*ctx == NULL) {
    return 0;
  }

  // check for the capacity, we need 4 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 4) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // next index
  iterator->queue.items[iterator->queue.count].ctx = ctx + 1;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_index;
  iterator->queue.items[iterator->queue.count].item_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].item_fn = item_fn;

  // close-index
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_index_close;

  // index content
  iterator->queue.items[iterator->queue.count].ctx = *ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = item_fn;

  // open-index
  iterator->queue.items[iterator->queue.count].ctx_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_index_open;

  // success
  return 0;
}

static i64 parquet_dump_array(struct parquet_metadata_iterator *iterator, u32 index) {
  void *ctx;
  u64 ctx_args, item_args;
  parquet_metadata_iterator_fn item_fn;

  // check for the capacity, we need 4 slots
  if (iterator->tokens.count > iterator->tokens.capacity - 4) {
    return PARQUET_ERROR_BUFFER_TOO_SMALL;
  }

  // check for the capacity, we need 4 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 4) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // copy values
  ctx = iterator->queue.items[index].ctx;
  item_fn = iterator->queue.items[index].item_fn;
  ctx_args = iterator->queue.items[index].ctx_args.value;
  item_args = iterator->queue.items[index].item_args.value;

  // key start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_KEY_START;
  iterator->tokens.items[iterator->tokens.count].type = DOM_TYPE_TEXT;
  iterator->tokens.items[iterator->tokens.count++].data = (u64) "text";

  // extract the name
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_LITERAL;
  iterator->tokens.items[iterator->tokens.count].data = ctx_args;
  iterator->tokens.items[iterator->tokens.count++].type = DOM_TYPE_TEXT;

  // key end
  iterator->tokens.items[iterator->tokens.count++].op = DOM_OP_KEY_END;

  // value start
  iterator->tokens.items[iterator->tokens.count].op = DOM_OP_VALUE_START;
  iterator->tokens.items[iterator->tokens.count++].data = item_args;

  // close-value
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_value_close;

  // close-array
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_array_close;

  // open-index with the first item or a null-termination
  iterator->queue.items[iterator->queue.count].ctx = ctx;
  iterator->queue.items[iterator->queue.count].ctx_args.value = ctx_args;
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_index;
  iterator->queue.items[iterator->queue.count].item_args.value = item_args;
  iterator->queue.items[iterator->queue.count++].item_fn = item_fn;

  // open-array
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_array_open;

  // success
  return 0;
}

static i64 parquet_dump_encoding_stats(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_page_encoding_stats *encoding_stats;

  // check for the capacity, we need 5 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 5) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  encoding_stats = (struct parquet_page_encoding_stats *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = encoding_stats;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding-stats";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // count
  if (encoding_stats->count != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->count;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "count";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // encoding
  if (encoding_stats->encoding != PARQUET_ENCODING_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->encoding;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding;
  }

  // page_type
  if (encoding_stats->page_type != PARQUET_PAGE_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &encoding_stats->page_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "page_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_page_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = encoding_stats;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding-stats";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_column_meta(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_meta *column_meta;

  // check for the capacity, we need 13 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 13) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  column_meta = (struct parquet_column_meta *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = column_meta;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-meta";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // encoding_stats
  if (column_meta->encoding_stats != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->encoding_stats;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encoding_stats";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding_stats;
  }

  // dictionary_page_offset
  if (column_meta->dictionary_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->dictionary_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "dictionary_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // index_page_offset
  if (column_meta->index_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->index_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "index_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // data_page_offset
  if (column_meta->data_page_offset != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->data_page_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_page_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_compressed_size
  if (column_meta->total_compressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->total_compressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_compressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_uncompressed_size
  if (column_meta->total_uncompressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->total_uncompressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_uncompressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // num_values
  if (column_meta->num_values != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->num_values;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_values";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // compression_codec
  if (column_meta->compression_codec != PARQUET_COMPRESSION_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->compression_codec;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "compression_codec";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_compression_codec;
  }

  // path_in_schema
  if (column_meta->path_in_schema != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->path_in_schema;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "path_in_schema";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "str";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // encodings
  if (column_meta->encodings != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_meta->encodings;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "encodings";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_encoding;
  }

  // data_type
  if (column_meta->data_type != PARQUET_DATA_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &column_meta->data_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_data_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = column_meta;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-meta";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_column_chunk(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_column_chunk *column_chunk;

  // check for the capacity, we need 5 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 5) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  column_chunk = (struct parquet_column_chunk *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = column_chunk;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-chunk";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // meta
  if (column_chunk->meta != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_chunk->meta;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "meta";
    iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_column_meta;
  }

  // file_path
  if (column_chunk->file_path != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = column_chunk->file_path;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_path";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // file_offset, not -1
  if (column_chunk->file_offset > 0) {
    iterator->queue.items[iterator->queue.count].ctx = &column_chunk->file_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = column_chunk;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "column-chunk";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_row_group(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_row_group *row_group;

  // check for the capacity, we need 7 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 7) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  row_group = (struct parquet_row_group *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = row_group;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "row_group";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // total_compressed_size
  if (row_group->total_compressed_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->total_compressed_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_compressed_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // file_offset, not -1
  if (row_group->file_offset > 0) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->file_offset;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "file_offset";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // num_rows
  if (row_group->num_rows != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->num_rows;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_rows";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // total_byte_size
  if (row_group->total_byte_size != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &row_group->total_byte_size;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "total_byte_size";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // columns
  if (row_group->columns != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = row_group->columns;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "columns";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_column_chunk;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = row_group;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "row_group";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  return 0;
}

static i64 parquet_dump_schema_element(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_schema_element *schema_element;

  // check for the capacity, we need 8 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 8) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  schema_element = (struct parquet_schema_element *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = schema_element;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "schema_element";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // converted_type
  if (schema_element->converted_type != PARQUET_CONVERTED_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->converted_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "converted_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_converted_type;
  }

  // num_children
  if (schema_element->num_children != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->num_children;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_children";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // name
  if (schema_element->name != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = schema_element->name;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "name";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // repetition_type
  if (schema_element->repetition_type != PARQUET_REPETITION_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->repetition_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "repetition_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_repetition_type;
  }

  // type_length
  if (schema_element->type_length != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->type_length;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "type_length";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // data_type
  if (schema_element->data_type != PARQUET_DATA_TYPE_NONE) {
    iterator->queue.items[iterator->queue.count].ctx = &schema_element->data_type;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "data_type";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "enum";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_data_type;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = schema_element;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "schema_element";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

static i64 parquet_dump_metadata(struct parquet_metadata_iterator *iterator, u32 index) {
  struct parquet_metadata *metadata;

  // check for the capacity, we need 7 slots in the queue
  if (iterator->queue.count > iterator->queue.capacity - 7) {
    return PARQUET_ERROR_CAPACITY_OVERFLOW;
  }

  // get the value
  metadata = (struct parquet_metadata *)iterator->queue.items[index].ctx;

  // close-struct
  iterator->queue.items[iterator->queue.count].ctx = metadata;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_close;

  // created_by
  if (metadata->created_by != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->created_by;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "created_by";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "text";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_text;
  }

  // row_groups
  if (metadata->row_groups != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->row_groups;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "row_groups";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_row_group;
  }

  // num_rows
  if (metadata->num_rows != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &metadata->num_rows;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "num_rows";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i64";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i64;
  }

  // schemas
  if (metadata->schemas != PARQUET_NULL_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = metadata->schemas;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "schemas";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_array;
    iterator->queue.items[iterator->queue.count].item_args.name = "struct";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_schema_element;
  }

  // version
  if (metadata->version != PARQUET_UNKNOWN_VALUE) {
    iterator->queue.items[iterator->queue.count].ctx = &metadata->version;
    iterator->queue.items[iterator->queue.count].ctx_args.name = "version";
    iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_field;
    iterator->queue.items[iterator->queue.count].item_args.name = "i32";
    iterator->queue.items[iterator->queue.count++].item_fn = parquet_dump_i32;
  }

  // open-struct
  iterator->queue.items[iterator->queue.count].ctx = metadata;
  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count++].ctx_fn = parquet_dump_struct_open;

  // success
  return 0;
}

void parquet_metadata_iter(struct parquet_metadata_iterator *iterator, struct parquet_metadata *metadata) {
  iterator->metadata = metadata;

  iterator->tokens.count = 0;
  iterator->tokens.capacity = PARQUET_METADATA_TOKENS_SIZE;

  iterator->queue.count = 0;
  iterator->queue.capacity = PARQUET_METADATA_QUEUE_SIZE;

  iterator->queue.items[iterator->queue.count].ctx_args.name = "metadata";
  iterator->queue.items[iterator->queue.count].ctx_fn = parquet_dump_metadata;
  iterator->queue.items[iterator->queue.count++].ctx = metadata;
}

i64 parquet_metadata_next(struct parquet_metadata_iterator *iterator) {
  u32 index;
  i64 result;
  parquet_metadata_iterator_fn fn;

  // reset the tokens counter
  iterator->tokens.count = 0;

  // iterator over the LIFO stack
  while (iterator->queue.count > 0) {
    index = --iterator->queue.count;
    fn = iterator->queue.items[index].ctx_fn;

    // call next function
    result = fn(iterator, index);

    // handle buffer too small error
    if (result == PARQUET_ERROR_BUFFER_TOO_SMALL) {
      iterator->queue.count++;
      break;
    };

    // propagate unhandled errors
    if (result < 0) return result;
  }

  // success
  return 0;
}

#if defined(I13C_TESTS)

static void can_iterate_through_metadata() {
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = 1;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = 43;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = "test_user";

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // iterate one batch
  result = parquet_metadata_next(&iterator);
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count > 0, "should have some tokens");
}

static void can_dump_enum_with_known_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_TEXT, "expected text");
  assert_eq_str((const char *)iterator.tokens.items[0].data, "INT96", "expected correct value");
}

static void can_dump_enum_with_unknown_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 27;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I32, "expected integer");
  assert((u64)iterator.tokens.items[0].data == 27, "expected correct value");
}

static void can_detect_buffer_too_small_with_enum() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;
  iterator.queue.items[0].ctx_args.name = "field-name";

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity;

  // process enum
  result = parquet_dump_enum(&iterator, 0, PARQUET_DATA_TYPE_SIZE, PARQUET_DATA_TYPE_NAMES);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_dump_literal_with_i32_value() {
  i32 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_I32);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I32, "expected i32");
  assert((i32)iterator.tokens.items[0].data == 3, "expected correct value");
}

static void can_dump_literal_with_i64_value() {
  i64 value;
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 3;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = &value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_I64);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_I64, "expected i64");
  assert((i64)iterator.tokens.items[0].data == 3, "expected correct value");
}

static void can_dump_literal_with_text_value() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = "abc";
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_TEXT);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 1, "should generate 1 token");

  assert(iterator.tokens.items[0].op == DOM_OP_LITERAL, "expected literal");
  assert(iterator.tokens.items[0].type == DOM_TYPE_TEXT, "expected text");
  assert_eq_str((const char *)iterator.tokens.items[0].data, "abc", "expected correct value");
}

static void can_detect_buffer_too_small_with_literal() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = "abc";
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity;

  // process literal
  result = parquet_dump_literal(&iterator, 0, DOM_TYPE_TEXT);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_dump_index_with_null_terminator() {
  i64 result;
  const char *value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = PARQUET_NULL_VALUE;
  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 0, "should generate 0 tokens");
  assert(iterator.queue.count == 0, "should have 0 items in the queue");
}

static void can_dump_index_with_next_item() {
  i64 result;
  const char *value[3];

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value[0] = "abc";
  value[1] = "cde";
  value[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 0, "should generate 0 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_index_open, "expected index open");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_index_close, "expected index close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_index, "expected index");
  assert(iterator.queue.items[2].ctx_fn == iterator.queue.items[0].item_fn, "expected callback");

  assert(iterator.queue.items[2].ctx == value[0], "expected first item");
  assert(iterator.queue.items[0].ctx == &value[1], "expected second item");
}

static void can_detect_capacity_overflow_with_index() {
  i64 result;
  const char *value[3];

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value[0] = "abc";
  value[1] = "cde";
  value[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = iterator.queue.capacity - 3;
  iterator.queue.items[0].ctx = (void *)value;
  iterator.queue.items[0].ctx_args.name = "type-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_index(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
  assert(iterator.tokens.count == 0, "should not generate any tokens");
  assert(iterator.queue.count == iterator.queue.capacity - 3, "should not change the queue");
}

static void can_dump_array_with_no_items() {
  i64 *array[1];
  i64 result;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_array_open, "expected array open");
  assert(iterator.queue.items[2].ctx_fn == parquet_dump_index, "expected array index");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_array_close, "expected array close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");

  assert(iterator.queue.items[2].ctx == array, "expected first item");
  assert((u64)iterator.queue.items[2].item_fn == 0x12345678, "expected callback");
  assert_eq_str(iterator.queue.items[2].item_args.name, "item-x", "expected correct name");
}

static void can_dump_array_with_two_items() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 4, "should have 4 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[3].ctx_fn == parquet_dump_array_open, "expected array open");
  assert(iterator.queue.items[2].ctx_fn == parquet_dump_index, "expected array index");
  assert(iterator.queue.items[1].ctx_fn == parquet_dump_array_close, "expected array close");
  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");

  assert(iterator.queue.items[2].ctx == array, "expected first item");
  assert((u64)iterator.queue.items[2].item_fn == 0x12345678, "expected callback");
  assert_eq_str(iterator.queue.items[2].item_args.name, "item-x", "expected correct name");
}

static void can_detect_buffer_too_small_with_array() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity - 3;

  // process array
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_detect_capacity_overflow_with_array() {
  i64 *array[3];
  i64 result, v1, v2;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  array[0] = &v1;
  array[1] = &v2;
  array[2] = PARQUET_NULL_VALUE;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&array;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.queue.count = iterator.queue.capacity - 3;

  // process array
  result = parquet_dump_array(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
}

static void can_dump_field_with_type_name() {
  i64 result, value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.name = "item-x";
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 2, "should have 2 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");
  assert(iterator.queue.items[1].ctx_fn == (parquet_metadata_iterator_fn)0x12345678, "expected callback");

  assert(iterator.queue.items[1].ctx == &value, "expected value");
  assert_eq_str(iterator.queue.items[1].ctx_args.name, "item-x", "expected correct name");
}

static void can_dump_field_with_type_id() {
  i64 result, value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // process index
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == 0, "should succeed");
  assert(iterator.tokens.count == 4, "should generate 4 tokens");
  assert(iterator.queue.count == 2, "should have 2 items in the queue");

  assert(iterator.tokens.items[0].op == DOM_OP_KEY_START, "expected key-start");
  assert(iterator.tokens.items[1].op == DOM_OP_LITERAL, "expected literal");
  assert_eq_str((const char *)iterator.tokens.items[1].data, "field-name", "expected correct key");
  assert(iterator.tokens.items[2].op == DOM_OP_KEY_END, "expected key-end");
  assert(iterator.tokens.items[3].op == DOM_OP_VALUE_START, "expected value-start");

  assert(iterator.queue.items[0].ctx_fn == parquet_dump_value_close, "expected value close");
  assert(iterator.queue.items[1].ctx_fn == (parquet_metadata_iterator_fn)0x12345678, "expected callback");

  assert(iterator.queue.items[1].ctx == &value, "expected value");
  assert(iterator.queue.items[1].ctx_args.value == DOM_TYPE_U16, "expected correct name");
}

static void can_detect_buffer_too_small_with_field() {
  i64 result;
  u64 value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.tokens.count = iterator.tokens.capacity - 3;

  // process field
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_BUFFER_TOO_SMALL, "should fail with PARQUET_ERROR_BUFFER_TOO_SMALL");
}

static void can_detect_capacity_overflow_with_field() {
  i64 result;
  u64 value;

  struct parquet_metadata metadata;
  struct parquet_metadata_iterator iterator;

  // initialize metadata
  metadata.version = PARQUET_UNKNOWN_VALUE;
  metadata.schemas = PARQUET_NULL_VALUE;
  metadata.num_rows = PARQUET_UNKNOWN_VALUE;
  metadata.row_groups = PARQUET_NULL_VALUE;
  metadata.created_by = PARQUET_NULL_VALUE;

  // initialize iterator
  parquet_metadata_iter(&iterator, &metadata);

  // alter it for this test case
  value = 42;

  iterator.queue.count = 0;
  iterator.queue.items[0].ctx = (void *)&value;
  iterator.queue.items[0].ctx_args.name = "field-name";
  iterator.queue.items[0].item_args.value = DOM_TYPE_U16;
  iterator.queue.items[0].item_fn = (parquet_metadata_iterator_fn)0x12345678;

  // we expect a bit more capacity
  iterator.queue.count = iterator.queue.capacity - 1;

  // process field
  result = parquet_dump_field(&iterator, 0);

  // assert results
  assert(result == PARQUET_ERROR_CAPACITY_OVERFLOW, "should fail with PARQUET_ERROR_CAPACITY_OVERFLOW");
}

void parquet_test_cases_iter(struct runner_context *ctx) {
  test_case(ctx, "can iterate through metadata", can_iterate_through_metadata);
  test_case(ctx, "can dump enum with known value", can_dump_enum_with_known_value);
  test_case(ctx, "can dump enum with unknown value", can_dump_enum_with_unknown_value);
  test_case(ctx, "can detect buffer too small with enum", can_detect_buffer_too_small_with_enum);

  test_case(ctx, "can dump literal with i32 value", can_dump_literal_with_i32_value);
  test_case(ctx, "can dump literal with i64 value", can_dump_literal_with_i64_value);
  test_case(ctx, "can dump literal with text value", can_dump_literal_with_text_value);
  test_case(ctx, "can detect buffer to small with literal", can_detect_buffer_too_small_with_literal);

  test_case(ctx, "can dump index with null-terminator", can_dump_index_with_null_terminator);
  test_case(ctx, "can dump index with next item", can_dump_index_with_next_item);
  test_case(ctx, "can detect buffer too small with index", can_detect_capacity_overflow_with_index);

  test_case(ctx, "can dump array with no items", can_dump_array_with_no_items);
  test_case(ctx, "can dump array with two items", can_dump_array_with_two_items);
  test_case(ctx, "can detect buffer too small with array", can_detect_buffer_too_small_with_array);
  test_case(ctx, "can detect capacity overflow with array", can_detect_capacity_overflow_with_array);

  test_case(ctx, "can dump field with type name", can_dump_field_with_type_name);
  test_case(ctx, "can dumo field with type id", can_dump_field_with_type_id);
  test_case(ctx, "can detect buffer too small with field", can_detect_buffer_too_small_with_field);
  test_case(ctx, "can detect capacity overflow with field", can_detect_capacity_overflow_with_field);
}

#endif
