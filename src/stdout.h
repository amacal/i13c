/// @brief prints a text in the stdout
/// @param len length of the text
/// @param data ptr to the text
/// @return negative error or 0 on success
extern long stdout_print(const long len, const char* data);

/// @brief print a formatted text in the stdout
/// @param fmt ptr to the format
/// @return a negative error or 0 on succcess
extern long stdout_printf(const char* fmt, ...);
