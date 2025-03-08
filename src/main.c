/// @brief prints a text in the stdout
/// @param len the length of the text
/// @param data the ptr to the text
/// @return a negative error or 0 on success
int stdout_print(const long len, const char* data);

/// @brief print a formatted text in the stdout
/// @param fmt the ptr to the format
/// @param arg1 the first substitute
/// @return a negative error or 0 on succcess
int stdout_printf(const char* fmt, const char* arg1);

int main() {
    stdout_printf("Hello, %s!\n\0", "Adrian\0");
}
