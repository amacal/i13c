#pragma once

#include "runner.h"
#include "typing.h"

enum parquet_data_type {
  PARQUET_DATA_TYPE_NONE = -1,
  PARQUET_DATA_TYPE_BOOLEAN = 0,
  PARQUET_DATA_TYPE_INT32 = 1,
  PARQUET_DATA_TYPE_INT64 = 2,
  PARQUET_DATA_TYPE_INT96 = 3,
  PARQUET_DATA_TYPE_FLOAT = 4,
  PARQUET_DATA_TYPE_DOUBLE = 5,
  PARQUET_DATA_TYPE_BYTE_ARRAY = 6,
  PARQUET_DATA_TYPE_BYTE_ARRAY_FIXED = 7,
  PARQUET_DATA_TYPE_SIZE,
};

enum parquet_repetition_type {
  PARQUET_REPETITION_TYPE_NONE = -1,
  PARQUET_REPETITION_TYPE_REQUIRED = 0,
  PARQUET_REPETITION_TYPE_OPTIONAL = 1,
  PARQUET_REPETITION_TYPE_REPEATED = 2,
  PARQUET_REPETITION_TYPE_SIZE,
};

enum parquet_converted_type {
  PARQUET_CONVERTED_TYPE_NONE = -1,
  PARQUET_CONVERTED_TYPE_UTF8 = 0,
  PARQUET_CONVERTED_TYPE_MAP = 1,
  PARQUET_CONVERTED_TYPE_MAP_KEY_VALUE = 2,
  PARQUET_CONVERTED_TYPE_LIST = 3,
  PARQUET_CONVERTED_TYPE_ENUM = 4,
  PARQUET_CONVERTED_TYPE_DECIMAL = 5,
  PARQUET_CONVERTED_TYPE_DATE = 6,
  PARQUET_CONVERTED_TYPE_TIME_MILLIS = 7,
  PARQUET_CONVERTED_TYPE_TIME_MICROS = 8,
  PARQUET_CONVERTED_TYPE_TIMESTAMP_MILLIS = 9,
  PARQUET_CONVERTED_TYPE_TIMESTAMP_MICROS = 10,
  PARQUET_CONVERTED_TYPE_UINT8 = 11,
  PARQUET_CONVERTED_TYPE_UINT16 = 12,
  PARQUET_CONVERTED_TYPE_UINT32 = 13,
  PARQUET_CONVERTED_TYPE_UINT64 = 14,
  PARQUET_CONVERTED_TYPE_INT8 = 15,
  PARQUET_CONVERTED_TYPE_INT16 = 16,
  PARQUET_CONVERTED_TYPE_INT32 = 17,
  PARQUET_CONVERTED_TYPE_INT64 = 18,
  PARQUET_CONVERTED_TYPE_JSON = 19,
  PARQUET_CONVERTED_TYPE_BSON = 20,
  PARQUET_CONVERTED_TYPE_INTERVAL = 21,
  PARQUET_CONVERTED_TYPE_SIZE,
};

enum parquet_encoding {
  PARQUET_ENCODING_NONE = -1,
  PARQUET_ENCODING_PLAIN = 0,
  PARQUET_ENCODING_GROUP_VAR_INT = 1,
  PARQUET_ENCODING_PLAIN_DICTIONARY = 2,
  PARQUET_ENCODING_RLE = 3,
  PARQUET_ENCODING_BIT_PACKED = 4,
  PARQUET_ENCODING_DELTA_BINARY_PACKED = 5,
  PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY = 6,
  PARQUET_ENCODING_DELTA_BYTE_ARRAY = 7,
  PARQUET_ENCODING_RLE_DICTIONARY = 8,
  PARQUET_ENCODING_BYTE_STREAM_SPLIT = 9,
  PARQUET_ENCODING_SIZE,
};

enum parquet_compression_codec {
  PARQUET_COMPRESSION_NONE = -1,
  PARQUET_COMPRESSION_UNCOMPRESSED = 0,
  PARQUET_COMPRESSION_SNAPPY = 1,
  PARQUET_COMPRESSION_GZIP = 2,
  PARQUET_COMPRESSION_LZO = 3,
  PARQUET_COMPRESSION_BROTLI = 4,
  PARQUET_COMPRESSION_LZ4 = 5,
  PARQUET_COMPRESSION_ZSTD = 6,
  PARQUET_COMPRESSION_LZ4_RAW = 7,
  PARQUET_COMPRESSION_SIZE,
};

enum parquet_page_type {
  PARQUET_PAGE_TYPE_NONE = -1,
  PARQUET_PAGE_TYPE_DATA_PAGE = 0,
  PARQUET_PAGE_TYPE_INDEX_PAGE = 1,
  PARQUET_PAGE_TYPE_DICTIONARY_PAGE = 2,
  PARQUET_PAGE_TYPE_DATA_PAGE_V2 = 3,
  PARQUET_PAGE_TYPE_SIZE,
};

struct parquet_schema_element {
  i32 data_type;       // 1, data type for this field, set only in leaf-node
  i32 type_length;     // 2, if type is FIXED_BYTE_ARRAY, this is the length of the array
  i32 repetition_type; // 3, repetition of the field, not set in the root-node
  char *name;          // 4, name of the schema element
  i32 num_children;    // 5, number of children in the schema
  i32 converted_type;  // 6, common types used by frameworks using parquet
};

struct parquet_column_statistics {
  u8 *max;                 // 1, maximum value in the column
  u8 *min;                 // 2, minimum value in the column
  i64 null_count;          // 3, number of null values in the column
  i64 distinct_count;      // 4, number of distinct values in the column
  u8 *max_value;           // 5, maximum value in the column as a byte array
  u8 *min_value;           // 6, minimum value in the column as a byte array
  bool is_max_value_exact; // 7, whether the max value is exact
  bool is_min_value_exact; // 8, whether the min value is exact
};

struct parquet_page_encoding_stats {
  i32 page_type; // 1, type of the page
  i32 encoding;  // 2, encoding used for the page
  i32 count;     // 3, number of pages with this encoding
};

struct parquet_column_meta {
  i32 data_type;                                       // 1, data type of this column
  i32 **encodings;                                     // 2, null-terminated array of encodings used for this column
  char **path_in_schema;                               // 3, path to the column in the schema, null-terminated
  i32 compression_codec;                               // 4, compression codec used for this column
  i64 num_values;                                      // 5, number of values in the column
  i64 total_uncompressed_size;                         // 6, total uncompressed size of the column
  i64 total_compressed_size;                           // 7, total compressed size of the column
  i64 data_page_offset;                                // 9, offset of the data page in the file
  i64 index_page_offset;                               // 10, offset of the index page in the file
  i64 dictionary_page_offset;                          // 11, offset of the dictionary page in the file
  struct parquet_column_statistics *statistics;        // 12, statistics for the column
  struct parquet_page_encoding_stats **encoding_stats; // 13, null-terminated array of encoding stats
};

struct parquet_column_chunk {
  char *file_path;                  // 1, optional, path to the external file
  i64 file_offset;                  // 2. file offset of the column chunk, obsolete
  struct parquet_column_meta *meta; // 3, metadata for the column chunk
};

struct parquet_row_group {
  struct parquet_column_chunk **columns; // 1, null-terminated array of column chunks
  i64 total_byte_size;                   // 2, total byte size of the row group
  i64 num_rows;                          // 3, number of rows in the row group
  i64 file_offset;                       // 5, file offset of the row group
  i64 total_compressed_size;             // 6, total compressed size of the row group
};

struct parquet_metadata {
  i32 version;                             // 1, parquet file version
  struct parquet_schema_element **schemas; // 2, null-terminated array of schema elements
  i64 num_rows;                            // 3, number of rows
  struct parquet_row_group **row_groups;   // 4, null-terminated array of row groups
  char *created_by;                        // 6, null-terminated created by string
};

/// @brief Parses the footer of a parquet file.
/// @param file Pointer to the parquet_file structure.
/// @param metadata Pointer to the parquet_metadata structure to fill.
/// @return 0 on success, or a negative error code on failure.
extern i64 parquet_parse(struct parquet_file *file, struct parquet_metadata *metadata);

#if defined(I13C_TESTS)

/// @brief Registers parquet test cases.
/// @param ctx Pointer to the runner_context structure.
extern void parquet_test_cases_parse(struct runner_context *ctx);

#endif
