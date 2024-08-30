#pragma once
#include <cstdio>
#include <cstdarg>
#include <sys/time.h>

#define MAX_FORMAT_LENGTH 255
#define DEBUG (false)
#define TITLE (false)
#define TIMER (false)
#define CUR (false)

#define DEBUG_ENABLE (true)

class debug{
    public:
        static void debug_title(const char* str){
            if(TITLE)
                printf("\033[0;45;1m%s\033[0m\n", str);
        }

        static void debug_item(const char* format, ...){
            char _format[MAX_FORMAT_LENGTH];

            va_list args;
            va_start(args, format);
            if(DEBUG_ENABLE){
                sprintf(_format, "\033[0;42;1m%s\033[0m\n", format); /* Wrap format in a style. */
                vprintf(_format, args); /* Print string of debug item. */
            }
            va_end(args);
        }

        static void debug_cur(const char* format, ...){
            char _format[MAX_FORMAT_LENGTH];

            va_list args;
            va_start(args, format);
            if(CUR){
                sprintf(_format, "%s\n", format); /* Wrap format in a style. */
                vprintf(_format, args); /* Print string of debug item. */
            }
            va_end(args);
        }

        static void notify_info(const char* format, ...){
            char _format[MAX_FORMAT_LENGTH];

            va_list args;
            va_start(args, format);
            sprintf(_format, "\033[4m%s\033[0m\n", format); /* Wrap format in a style. */
            vprintf(_format, args); /* Print string of debug item. */
	    fflush(stdout);
            va_end(args);
        }

        static void notify_error(const char* format, ...){
            char _format[MAX_FORMAT_LENGTH];

            va_list args;
            va_start(args, format);
            sprintf(_format, "\033[0;31m%s\033[0m\n", format); /* Wrap format in a style. */
            vprintf(_format, args); /* Print string of debug item. */
	    fflush(stdout);
            va_end(args);
        }

    private:
        static long start_time;
};
