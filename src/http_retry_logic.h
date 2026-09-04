/*-
 * HTTP source retry / error-tracking logic.
 *
 * Tracks consecutive 5xx failures within a time window so the source can
 * revert to the original (pre-redirect) URL after repeated failures on a
 * redirect target (e.g. stale Acestream engine session).
 */
#ifndef __HTTP_RETRY_LOGIC_H__
#define __HTTP_RETRY_LOGIC_H__

#include <time.h>

#include "stream_src.h"

/* Number of consecutive 5xx errors (within the window) after which the
 * source reverts to the original URL. */
#define HTTP_RETRY_ERR_THRESHOLD	(3)
/* Window (seconds): errors older than that do not count as consecutive. */
#define HTTP_RETRY_ERR_WINDOW_SEC	(30)

/* Set retry tracking defaults (also (re)initializes all fields). */
void	http_retry_init(str_src_conn_http_p http);

/* Record one 5xx failure: maintains consecutive_errors within the
 * configured time window (http_retry_error_window_sec). */
void	http_record_failure(str_src_conn_http_p http);

/* Non zero - the error threshold is reached, revert is advised. */
int	http_should_revert(str_src_conn_http_p http);

/* Reset the error counter (on success or after a successful revert). */
void	http_reset_error_counter(str_src_conn_http_p http);

/* Suggested next retry time (simple linear back-off), 0 if no failures. */
time_t	http_next_retry_time(str_src_conn_http_p http);

#endif /* __HTTP_RETRY_LOGIC_H__ */
