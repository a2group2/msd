/*-
 * HTTP source retry / error-tracking logic.
 * See http_retry_logic.h for the API description.
 */

#include <stdlib.h> /* NULL */
#include <string.h> /* memcpy (io_buf.h). */
#include <errno.h> /* EINVAL, ENOMEM (io_buf.h). */

#include "http_retry_logic.h"

void
http_retry_init(str_src_conn_http_p http) {

	if (NULL == http)
		return;
	http->consecutive_errors = 0;
	http->last_failure_time = 0;
	http->error_window_sec = HTTP_RETRY_ERR_WINDOW_SEC;
	http->is_original_url = 1; /* Start at the user-configured URL. */
}

void
http_record_failure(str_src_conn_http_p http) {
	time_t now;
	uint32_t win;

	if (NULL == http)
		return;
	now = time(NULL);
	win = ((0 != http->error_window_sec) ?
	    http->error_window_sec : (uint32_t)HTTP_RETRY_ERR_WINDOW_SEC);
	/* Errors outside the window are not consecutive: start over. */
	if (0 != http->last_failure_time &&
	    (now - http->last_failure_time) > (time_t)win) {
		http->consecutive_errors = 0;
	}
	if (255 > http->consecutive_errors) {
		http->consecutive_errors ++;
	}
	http->last_failure_time = now;
}

int
http_should_revert(str_src_conn_http_p http) {

	if (NULL == http)
		return (0);
	return (HTTP_RETRY_ERR_THRESHOLD <= http->consecutive_errors);
}

void
http_reset_error_counter(str_src_conn_http_p http) {

	if (NULL == http)
		return;
	http->consecutive_errors = 0;
	http->last_failure_time = 0;
}

time_t
http_next_retry_time(str_src_conn_http_p http) {

	if (NULL == http || 0 == http->consecutive_errors)
		return (0);
	/* Linear back-off: 2s after the 1st error, 4s after the 2nd, ... */
	return (http->last_failure_time +
	    (time_t)(2 * http->consecutive_errors));
}
