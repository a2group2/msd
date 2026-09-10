/*-
 * multicast_worker.h - Thread-per-core, shared-nothing multicast receiver.
 *
 * A small, self-contained, production-grade module that receives UDP
 * (multicast or unicast) datagrams on a pool of worker threads, one
 * epoll instance per thread, one thread pinned per CPU core.
 *
 * Design:
 *  - Each worker owns: one epoll fd, one eventfd (stop/wake), one
 *    pre-allocated recvmmsg() batch (buffers + iovecs + cmsg space).
 *    Nothing in the packet path allocates, locks or shares state.
 *  - Sockets are assigned to workers at registration time (control
 *    path only) by round-robin or explicit worker index; a socket is
 *    registered into exactly one worker's epoll instance.
 *  - Global statistics use C11 atomics.
 *  - Stop is a single atomic store + eventfd write (async-signal-safe).
 *
 * Copyright: public domain / BSD-0, use as you see fit.
 */
#ifndef MC_WORKER_H
#define MC_WORKER_H

#include <netinet/in.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Tunables (compile-time). */
#define MCW_BATCH_MAX     16u    /* recvmmsg() batch size.                  */
#define MCW_BUF_SIZE      2048u  /* Per-packet buffer (>= your max MTU payload). */
#define MCW_CTL_SIZE      128u   /* cmsg space per message (IP_PKTINFO).    */
#define MCW_MAX_SOCKETS   4096u  /* Hard cap of registered sockets.         */
#define MCW_EV_BATCH      64u    /* epoll_wait() batch size.                */

/* Global (aggregated) statistics, updated with relaxed atomics. */
typedef struct mcw_stats_s {
	_Atomic uint64_t	packets;
	_Atomic uint64_t	bytes;
	_Atomic uint64_t	errors;   /* recv errors / dropped msgs.      */
	_Atomic uint64_t	epolls;   /* epoll_wait() calls (diag).       */
} mcw_stats_t;

typedef struct mcw_worker_s	mcw_worker_t;
typedef struct mcw_pool_s	mcw_pool_t;

/*
 * Invoked on the owning worker thread for every received datagram.
 * "data" points into the worker's private receive buffer and is valid
 * only until the callback returns. "if_index" is the incoming
 * interface index (from IP_PKTINFO), 0 if unavailable.
 */
typedef void (*mcw_on_packet_cb)(void *udata, const uint8_t *data,
    size_t size, const struct sockaddr_storage *src, int if_index);

/*
 * Create a pool of "workers" workers, optionally pinned one-per-CPU
 * using the cpu_ids array (size == workers, NULL = auto: CPU 0..N-1).
 * Threads are NOT started until mcw_pool_start().
 * Returns 0 on success, errno-like value on failure.
 */
int	mcw_pool_create(mcw_pool_t **pool_ret, int workers,
	    const int *cpu_ids, mcw_on_packet_cb on_packet, void *udata);

/* Start worker threads. Idempotent. */
int	mcw_pool_start(mcw_pool_t *pool);

/*
 * Request graceful stop of all workers. Async-signal-safe: may be
 * called from a signal handler. Does not join threads.
 */
void	mcw_pool_stop(mcw_pool_t *pool);

/*
 * Join all worker threads, close epoll/event fds and sockets owned by
 * the pool, free all memory. Call after mcw_pool_stop().
 */
void	mcw_pool_destroy(mcw_pool_t *pool);

/*
 * Register an existing (non-blocking) datagram socket into the pool.
 * The socket is assigned to a worker using round-robin and added to
 * that worker's epoll instance. Socket is NOT closed on destroy.
 */
int	mcw_socket_add(mcw_pool_t *pool, int fd, int *worker_idx_ret);

/* Same, but explicit worker index (for IP-hash based placement). */
int	mcw_socket_add_to(mcw_pool_t *pool, int worker_idx, int fd);

/* Remove and (only) close sockets previously opened by
 * mcw_multicast_open(). For external sockets use epoll_ctl DEL +
 * close() yourself. */
int	mcw_multicast_close(mcw_pool_t *pool, int fd);

/*
 * Open, bind (INADDR_ANY:port, SO_REUSEADDR|SO_REUSEPORT|NONBLOCK|
 * CLOEXEC), join multicast group "group_ip" on interface if_index
 * (IP_ADD_MEMBERSHIP via ip_mreqn), enable IP_PKTINFO, and register
 * the socket on a round-robin selected worker.
 * The socket is owned by the pool and closed on mcw_pool_destroy().
 */
int	mcw_multicast_open(mcw_pool_t *pool, const char *group_ip,
	    uint16_t port, unsigned int if_index,
	    int *fd_ret, int *worker_idx_ret);

/* Aggregate (sum over workers) statistics snapshot. */
void	mcw_pool_stats_get(const mcw_pool_t *pool, mcw_stats_t *st_ret);

/* Per-worker socket count (for monitoring/diagnostics). */
int	mcw_worker_sockets(const mcw_pool_t *pool, int worker_idx);

#ifdef __cplusplus
}
#endif
#endif /* MC_WORKER_H */