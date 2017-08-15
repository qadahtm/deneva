//
// Created by Thamir Qadah on 7/26/17.
//

/*
 * logging.cpp
 *
 *  Created on: Jan 7, 2016
 *      Author: yaochang
 */

#include "util/lads_logging.h"
#include "concurrency_control/lads.h"

using namespace gdgcc;

namespace gdgcc {

/*
 * static variable members
 * */
    int Log::level_s = Log::DEBUG;
    FILE *Log::fp_s = stdout;
    pthread_mutex_t Log::m_s = PTHREAD_MUTEX_INITIALIZER;

    char Log::indicator[] = "FEWID";

    void Log::set_loglevel(int level) {
        Pthread_mutex_lock(&m_s);
        level_s = level;
        Pthread_mutex_unlock(&m_s);
    }

    int Log::get_loglevel() {
        int ret = 0;
        Pthread_mutex_lock(&m_s);
        ret = level_s;
        Pthread_mutex_unlock(&m_s);
        return ret;
    }

    void Log::set_logfile(FILE *fp) {
        Pthread_mutex_lock(&m_s);
        fp_s = fp;
        Pthread_mutex_unlock(&m_s);
    }

    FILE *Log::get_logfile() {
        FILE *ret = NULL;
        Pthread_mutex_lock(&m_s);
        ret = fp_s;
        Pthread_mutex_unlock(&m_s);
        return ret;
    }

    void Log::log_v(int level, int line, char *filename, char *fmt, va_list args) {
        assert(level >= Log::FATAL && level <= Log::DEBUG);
        if (level <= level_s) {
            char *filebase = basename(filename);
            char now_str[TIME_NOW_STR_SIZE];
            gdgcc::time_now_str(now_str);
            Pthread_mutex_lock(&m_s);
            fprintf(fp_s, "%c ", indicator[level]);
            if (filebase != NULL) {
                fprintf(fp_s, "[%s:%d]", filebase, line);
            }
            fprintf(fp_s, "%s |", now_str);
            vfprintf(fp_s, fmt, args);
            fprintf(fp_s, "\n");
            Pthread_mutex_unlock(&m_s);
            fflush(fp_s);
        }
    }

    void Log::log(int level, int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(level, line, filename, fmt, args);
        va_end(args);
    }

    void Log::fatal(int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::FATAL, line, filename, fmt, args);
        va_end(args);
        //Aborts the current process, producing an abnormal program termination.
        abort();
    }

    void Log::error(int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::ERROR, line, filename, fmt, args);
        va_end(args);
    }

    void Log::warn(int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::WARN, line, filename, fmt, args);
        va_end(args);
    }

    void Log::info(int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::INFO, line, filename, fmt, args);
        va_end(args);
    }

    void Log::debug(int line, char *filename, char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::DEBUG, line, filename, fmt, args);
        va_end(args);
    }

    void Log::fatal(char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::FATAL, 0, NULL, fmt, args);
        va_end(args);
        //Aborts the current process, producing an abnormal program termination.
        abort();
    }

    void Log::error(char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::ERROR, 0, NULL, fmt, args);
        va_end(args);
    }

    void Log::warn(char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::WARN, 0, NULL, fmt, args);
        va_end(args);
    }

    void Log::info(char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::INFO, 0, NULL, fmt, args);
        va_end(args);
    }

    void Log::debug(char *fmt, ...) {
        va_list args;
        va_start(args, fmt);
        log_v(Log::DEBUG, 0, NULL, fmt, args);
        va_end(args);
    }


}//end namespace gdgcc
