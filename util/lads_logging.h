//
// Created by Thamir Qadah on 7/26/17.
//

#ifndef DENEVA_LADS_LOGGING_H
#define DENEVA_LADS_LOGGING_H

#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/time.h>

namespace gdgcc {

/*
 * pshared means whether shared among different processes
 * */
#define Pthread_spin_init(l, pshared)    assert(pthread_spin_init(l, (pshared)) == 0)
#define Pthread_spin_lock(l)            assert(pthread_spin_lock(l) == 0)
#define Pthread_spin_unlock(l)            assert(pthread_spin_unlock(l) == 0)
#define Pthread_spin_destroy(l)            assert(pthread_spin_destroy(l) == 0)

#define Pthread_mutex_init(m, attr)        assert(pthread_mutex_init(m, attr) == 0)
#define Pthread_mutex_lock(m)            assert(pthread_mutex_lock(m) == 0)
#define Pthread_mutex_unlock(m)            assert(pthread_mutex_unlock(m) == 0)
#define Pthread_mutex_destroy(m)        assert(pthread_mutex_destroy(m) == 0)

#define Pthread_cond_init(c, attr)        assert(pthread_cond_init(c, attr) == 0)
#define Pthread_cond_destroy(c)            assert(pthread_cond_destroy(c) == 0)
#define Pthread_cond_signal(c)            assert(pthread_cond_signal(c) == 0)
#define Pthread_cond_broadcast(c)        assert(pthread_cond_broadcast(c) == 0)
#define Pthread_cond_wait(c, m)            assert(pthread_cond_wait(c, m) == 0)

#define Pthread_create(th, attr, func, arg) assert(pthread_create(th, attr, func, arg) == 0)
#define Pthread_join(th, attr)            assert(pthread_join(th, attr) == 0)


#define TIME_NOW_STR_SIZE 24

    static void time_now_str(char *now) UNUSED;
    static char* basename(char* fpath) UNUSED;


    static void make_int(char *str, int val, int digits) {
        char *p = str + digits;
        for (int i = 0; i < digits; i++) {
            int d = val % 10;
            val /= 10;
            p--;
            *p = '0' + d;
        }
    }

    static void time_now_str(char *now) {
        time_t seconds_since_epoch = time(NULL);
        struct tm local_calendar;
        localtime_r(&seconds_since_epoch, &local_calendar);
        make_int(now, local_calendar.tm_year + 1900, 4);
        now[4] = '-';
        make_int(now + 5, local_calendar.tm_mon + 1, 2);
        now[7] = '-';
        make_int(now + 8, local_calendar.tm_mday, 2);
        now[10] = ' ';
        make_int(now + 11, local_calendar.tm_hour, 2);
        now[13] = ':';
        make_int(now + 14, local_calendar.tm_min, 2);
        now[16] = ':';
        make_int(now + 17, local_calendar.tm_sec, 2);
        now[19] = '.';
        struct timeval tv;
        gettimeofday(&tv, NULL);
        make_int(now + 20, tv.tv_usec / 1000, 3);
        now[23] = '\0';
    }


    static char *basename(char *fpath) {
        if (fpath == NULL) {
            return NULL;
        }
        char sep_char = '/';
        int len = strlen(fpath);
        int idx = len - 1;
        while (idx > 0) {
            if (fpath[idx - 1] == sep_char) {
                break;
            }
            idx--;
        }
        return &(fpath[idx]);
    }


    class Log {
    private:
        /*
         * private static variable should define in header file
         * and initialize in source file.
         * */
        static int level_s;
        static FILE *fp_s;
        static pthread_mutex_t m_s;

        /*
         * va_list is a complete object type suitable for holding the information needed by
         * the macros va_start, va_copy, va_arg, and va_end.
         * */
        static void log_v(int level, int line, char *filename, const char *fmt, va_list args);

    public:
        enum {
            FATAL = 0, ERROR = 1, WARN = 2, INFO = 3, DEBUG = 4
        };
        static char indicator[];

        static void set_logfile(FILE *fp);

        static FILE *get_logfile();

        static void set_loglevel(int level);

        static int get_loglevel();

        static void log(int level, int line, char *filename, const char *fmt, ...);

        static void fatal(int line, char *filename, const char *fmt, ...);

        static void error(int line, char *filename, const char *fmt, ...);

        static void warn(int line, char *filename, const char *fmt, ...);

        static void info(int line, char *filename, const char *fmt, ...);

        static void debug(int line, char *filename, const char *fmt, ...);

        static void fatal(const char *fmt, ...);

        static void error(const char *fmt, ...);

        static void warn(const char *fmt, ...);

        static void info(const char *fmt, ...);

        static void debug(const char *fmt, ...);
    }; //end class Log

} //end namespace gdgcc
#endif //DENEVA_LADS_LOGGING_H
