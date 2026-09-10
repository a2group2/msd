/*-
 * multicast_worker.c - Thread-per-core, shared-nothing multicast receiver.
 * See multicast_worker.h for the API and design description.
 *
 * Packet path guarantees:
 *  - No locks, no allocations: recvmmsg() batches are pre-allocated once
 *    per worker and owned exclusively by the worker thread.
 *  - Each worker has a private epoll instance; a socket is registered
 *    into exactly one worker's epoll.
 *  - Global stats are updated with relaxed atomic RMW operations.
 *  - Stop = one atomic store + one eventfd write (async-signal-safe).
 */
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

#include "multicast_worker.h"

#define MCW_NAME_MAX	32

struct mcw_worker_s {
	int			epoll_fd;	/* Own epoll instance.     */
	int			stop_efd;	/* eventfd: stop/wake.     */
	pthread_t		thread;
	int			cpu;		/* Pinned CPU (-1 = any).  */
	_Atomic int		running;
	int			started;	/* pthread_create() done.  */

	mcw_on_packet_cb	cb;
	void			*udata;
	mcw_pool_t		*pool;
	mcw_stats_t		stats;

	/* recvmmsg() scratch, worker-thread private. */
	struct mmsghdr		*mmsg;
	struct iovec		*iov;
	uint8_t			**bufs;
	struct sockaddr_storage	*peers;
	char			*ctl;		/* Cmsg space, contiguous. */

	/* Registered fd list (control path only). */
	pthread_mutex_t		reg_lock;
	int			*fds;
	int			fd_cnt;
	int			fd_cap;
};

struct mcw_map_e {
	int	fd;
	int	widx;
	int	owned;
};

struct mcw_pool_s {
	mcw_worker_t		*workers;
	int			workers_cnt;
	int			started;

	mcw_stats_t		stats;		/* Global aggregated.      */

	pthread_mutex_t		map_lock;	/* fd -> worker map.       */
	struct mcw_map_e	*map;
	size_t			map_cnt;
	size_t			map_cap;

	_Atomic int		next_rr;	/* Round-robin cursor.     */
};

/* Wake a worker: one eventfd write (async-signal-safe). */
static void
mcw_wake_worker(const mcw_worker_t *w) {
	uint64_t one = 1;
	ssize_t r = write(w->stop_efd, &one, sizeof(one));

	(void)r;
}/* Extract incoming interface index from IP_PKTINFO cmsg. */
static int
mcw_parse_if_index(const struct msghdr *mh) {
	struct msghdr mh2 = *mh; /* CMSG_NXTHDR needs non-const. */
	struct cmsghdr *cm;

	for (cm = CMSG_FIRSTHDR(&mh2); NULL != cm;
	    cm = CMSG_NXTHDR(&mh2, cm)) {
		if (IPPROTO_IP == cm->cmsg_level &&
		    IP_PKTINFO == cm->cmsg_type &&
		    sizeof(struct in_pktinfo) <=
		    (size_t)(cm->cmsg_len - sizeof(struct cmsghdr))) {
			const struct in_pktinfo *pi =
			    (const struct in_pktinfo *)CMSG_DATA(cm);

			return (pi->ipi_ifindex);
		}
	}
	return (0);
}

/* Process one recvmmsg() batch. No locks, no allocations. Worker
 * stats only: mcw_pool_stats_get() aggregates over workers. */
static void
mcw_worker_recv(mcw_worker_t *w, int fd) {
	for (;;) { /* Drain until EAGAIN or partial batch. */
		int n = (int)recvmmsg(fd, w->mmsg, MCW_BATCH_MAX,
		    MSG_DONTWAIT, NULL);

		if (-1 == n) {
			if (EINTR == errno)
				continue;
			if (EAGAIN == errno || EWOULDBLOCK == errno)
				return; /* Drained. */
			atomic_fetch_add_explicit(&w->stats.errors, 1,
			    memory_order_relaxed);
			return;
		}
		if (0 == n)
			return;
		for (int i = 0; i < n; i ++) {
			const struct mmsghdr *mh = &w->mmsg[i];
			size_t size = (size_t)mh->msg_len;
			int if_index = mcw_parse_if_index(&mh->msg_hdr);

			if (0 == size)
				continue;
			atomic_fetch_add_explicit(&w->stats.packets, 1,
			    memory_order_relaxed);
			if (0 != (mh->msg_hdr.msg_flags & MSG_TRUNC)) {
				atomic_fetch_add_explicit(&w->stats.errors, 1,
				    memory_order_relaxed);
			} else {
				atomic_fetch_add_explicit(&w->stats.bytes,
				    (uint64_t)size, memory_order_relaxed);
			}
			w->cb(w->udata, w->bufs[i], size, &w->peers[i],
			    if_index);
		}
		if (n < (int)MCW_BATCH_MAX) /* Kernel queue drained. */
			return;
	}
}

/* The worker loop: lock-free, zero allocations, syscalls limited to
 * epoll_wait / recvmmsg / read(eventfd drain). */
static void
mcw_worker_loop(mcw_worker_t *w) {
	struct epoll_event evs[MCW_EV_BATCH];

	while (0 != atomic_load_explicit(&w->running,
	    memory_order_acquire)) {
		int n = epoll_wait(w->epoll_fd, evs, MCW_EV_BATCH, -1);

		atomic_fetch_add_explicit(&w->stats.epolls, 1,
		    memory_order_relaxed);
		if (-1 == n) {
			if (EINTR == errno)
				continue;
			atomic_fetch_add_explicit(&w->stats.errors, 1,
			    memory_order_relaxed);
			break;
		}
		for (int i = 0; i < n; i ++) {
			int fd = (int)(intptr_t)evs[i].data.fd;

			if (w->stop_efd == fd) { /* Stop wake: exit. */
				uint64_t tmp;

				while (sizeof(tmp) == read(w->stop_efd,
				    &tmp, sizeof(tmp))) {
					; /* Drain the eventfd. */
				}
				return;
			}
			if (0 != (EPOLLIN & evs[i].events))
				mcw_worker_recv(w, fd);
			/* EPOLLERR/EPOLLHUP: errors counted in recv. */
		}
	}
}

static void *
mcw_worker_main(void *arg) {
	mcw_worker_t *w = (mcw_worker_t *)arg;
	char name[MCW_NAME_MAX];

	/* Pin this worker to its dedicated CPU core. */
	if (0 <= w->cpu) {
		cpu_set_t cpuset;

		CPU_ZERO(&cpuset);
		CPU_SET(w->cpu, &cpuset);
		if (0 != pthread_setaffinity_np(pthread_self(),
		    sizeof(cpu_set_t), &cpuset)) {
			/* Non-fatal: continue unpinned. */
		}
	}
	snprintf(name, sizeof(name), "mcw:%i", w->cpu);
	pthread_setname_np(w->thread, name);
	mcw_worker_loop(w);
	return (NULL);
}/* Allocate the recvmmsg() scratch once, before thread start; owned
 * exclusively by the worker thread afterwards. */
static int
mcw_worker_rx_alloc(mcw_worker_t *w) {
	w->bufs = calloc(MCW_BATCH_MAX, sizeof(uint8_t *));
	w->peers = calloc(MCW_BATCH_MAX, sizeof(struct sockaddr_storage));
	w->iov = calloc(MCW_BATCH_MAX, sizeof(struct iovec));
	w->mmsg = calloc(MCW_BATCH_MAX, sizeof(struct mmsghdr));
	w->ctl = calloc(MCW_BATCH_MAX, MCW_CTL_SIZE);
	if (NULL == w->bufs || NULL == w->peers || NULL == w->iov ||
	    NULL == w->mmsg || NULL == w->ctl)
		return (ENOMEM);
	for (size_t i = 0; i < MCW_BATCH_MAX; i ++) {
		w->bufs[i] = malloc(MCW_BUF_SIZE);
		if (NULL == w->bufs[i])
			return (ENOMEM);
		w->iov[i].iov_base = w->bufs[i];
		w->iov[i].iov_len = MCW_BUF_SIZE;
		w->mmsg[i].msg_hdr.msg_iov = &w->iov[i];
		w->mmsg[i].msg_hdr.msg_iovlen = 1;
		w->mmsg[i].msg_hdr.msg_name = &w->peers[i];
		w->mmsg[i].msg_hdr.msg_namelen =
		    sizeof(struct sockaddr_storage);
		w->mmsg[i].msg_hdr.msg_control = w->ctl + (i * MCW_CTL_SIZE);
		w->mmsg[i].msg_hdr.msg_controllen = MCW_CTL_SIZE;
	}
	return (0);
}

static void
mcw_worker_rx_free(mcw_worker_t *w) {
	if (NULL == w->bufs)
		return;
	for (size_t i = 0; i < MCW_BATCH_MAX; i ++)
		free(w->bufs[i]);
	free(w->bufs);
	free(w->peers);
	free(w->iov);
	free(w->mmsg);
	free(w->ctl);
	w->bufs = NULL;
	w->peers = NULL;
	w->iov = NULL;
	w->mmsg = NULL;
	w->ctl = NULL;
}

static int
mcw_worker_reg_add(mcw_worker_t *w, int fd) {
	int error = 0;

	pthread_mutex_lock(&w->reg_lock);
	if (w->fd_cnt == w->fd_cap) {
		int nc = ((0 == w->fd_cap) ? 16 : (w->fd_cap * 2));
		int *nf = realloc(w->fds, (sizeof(int) * (size_t)nc));

		if (NULL == nf) {
			error = ENOMEM;
		} else {
			w->fds = nf;
			w->fd_cap = nc;
		}
	}
	if (0 == error)
		w->fds[w->fd_cnt ++] = fd;
	pthread_mutex_unlock(&w->reg_lock);
	return (error);
}

static void
mcw_worker_reg_del(mcw_worker_t *w, int fd) {
	pthread_mutex_lock(&w->reg_lock);
	for (int i = 0; i < w->fd_cnt; i ++) {
		if (w->fds[i] == fd) {
			w->fds[i] = w->fds[-- w->fd_cnt];
			break;
		}
	}
	pthread_mutex_unlock(&w->reg_lock);
}

int
mcw_pool_create(mcw_pool_t **pool_ret, int workers,
    const int *cpu_ids, mcw_on_packet_cb on_packet, void *udata) {
	mcw_pool_t *pool;

	if (NULL == pool_ret || 0 >= workers || NULL == on_packet)
		return (EINVAL);
	pool = calloc(1, sizeof(mcw_pool_t));
	if (NULL == pool)
		return (ENOMEM);
	pool->workers = calloc((size_t)workers, sizeof(mcw_worker_t));
	if (NULL == pool->workers) {
		free(pool);
		return (ENOMEM);
	}
	pool->workers_cnt = workers;
	pthread_mutex_init(&pool->map_lock, NULL);
	for (int i = 0; i < workers; i ++) {
		mcw_worker_t *w = &pool->workers[i];

		w->cpu = ((NULL != cpu_ids) ? cpu_ids[i] : i);
		w->epoll_fd = -1;
		w->stop_efd = -1;
		w->pool = pool;
		w->cb = on_packet;
		w->udata = udata;
		pthread_mutex_init(&w->reg_lock, NULL);
	}
	(*pool_ret) = pool;
	return (0);
}/* Create epoll + eventfd + rx scratch for one worker. */
static int
mcw_worker_init(mcw_worker_t *w) {
	int error;
	struct epoll_event ev;

	w->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (-1 == w->epoll_fd) {
		error = errno;
		goto err_out;
	}
	w->stop_efd = eventfd(0, (EFD_CLOEXEC | EFD_NONBLOCK));
	if (-1 == w->stop_efd) {
		error = errno;
		goto err_out;
	}
	memset(&ev, 0x00, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.fd = w->stop_efd;
	if (0 != epoll_ctl(w->epoll_fd, EPOLL_CTL_ADD, w->stop_efd, &ev)) {
		error = errno;
		goto err_out;
	}
	error = mcw_worker_rx_alloc(w);
	if (0 != error)
		goto err_out;
	return (0);
err_out:
	if (-1 != w->stop_efd) { close(w->stop_efd); w->stop_efd = -1; }
	if (-1 != w->epoll_fd) { close(w->epoll_fd); w->epoll_fd = -1; }
	mcw_worker_rx_free(w);
	return (error);
}

/* Start workers; on any failure roll back cleanly (join started, free). */
int
mcw_pool_start(mcw_pool_t *pool) {
	if (NULL == pool || 0 != pool->started)
		return (EINVAL);
	for (int i = 0; i < pool->workers_cnt; i ++) {
		mcw_worker_t *w = &pool->workers[i];
		int error;

		error = mcw_worker_init(w);
		if (0 != error)
			goto err_out;
		atomic_store_explicit(&w->running, 1, memory_order_release);
		error = pthread_create(&w->thread, NULL, mcw_worker_main, w);
		if (0 != error) {
			atomic_store_explicit(&w->running, 0,
			    memory_order_release);
			goto err_out;
		}
		w->started = 1;
		continue;
err_out:
		/* Roll back: wake+join started workers, free resources. */
		atomic_store_explicit(&pool->workers[i].running, 0,
		    memory_order_release);
		for (int j = 0; j <= i; j ++) {
			mcw_worker_t *wj = &pool->workers[j];

			if (0 != wj->started) {
				mcw_wake_worker(wj);
				pthread_join(wj->thread, NULL);
				wj->started = 0;
			}
			if (-1 != wj->stop_efd) {
				close(wj->stop_efd);
				wj->stop_efd = -1;
			}
			if (-1 != wj->epoll_fd) {
				close(wj->epoll_fd);
				wj->epoll_fd = -1;
			}
			mcw_worker_rx_free(wj);
		}
		return (error);
	}
	pool->started = 1;
	return (0);
}

/* Async-signal-safe stop request. */
void
mcw_pool_stop(mcw_pool_t *pool) {
	if (NULL == pool)
		return;
	for (int i = 0; i < pool->workers_cnt; i ++) {
		mcw_worker_t *w = &pool->workers[i];

		atomic_store_explicit(&w->running, 0,
		    memory_order_release);
		if (-1 != w->stop_efd)
			mcw_wake_worker(w); /* write() is AS-safe. */
	}
}

void
mcw_pool_destroy(mcw_pool_t *pool) {
	if (NULL == pool)
		return;
	mcw_pool_stop(pool);
	for (int i = 0; i < pool->workers_cnt; i ++) {
		mcw_worker_t *w = &pool->workers[i];

		if (0 != w->started) {
			pthread_join(w->thread, NULL);
			w->started = 0;
		}
		if (-1 != w->stop_efd) {
			close(w->stop_efd);
			w->stop_efd = -1;
		}
		if (-1 != w->epoll_fd) {
			close(w->epoll_fd);
			w->epoll_fd = -1;
		}
		mcw_worker_rx_free(w);
		pthread_mutex_destroy(&w->reg_lock);
		free(w->fds);
		w->fds = NULL;
	}
	/* Close sockets owned by the pool (mcw_multicast_open). */
	pthread_mutex_lock(&pool->map_lock);
	for (size_t k = 0; k < pool->map_cnt; k ++) {
		if (0 != pool->map[k].owned)
			close(pool->map[k].fd);
	}
	pthread_mutex_unlock(&pool->map_lock);
	free(pool->map);
	pthread_mutex_destroy(&pool->map_lock);
	free(pool->workers);
	free(pool);
}/* Assign a socket to a worker: epoll_ctl ADD + map + worker fd list. */
static int
mcw_socket_add_worker(mcw_pool_t *pool, int widx, int fd, int owned) {
	mcw_worker_t *w = &pool->workers[widx];
	struct epoll_event ev;
	int error;

	memset(&ev, 0x00, sizeof(ev));
	ev.events = EPOLLIN;
	ev.data.fd = fd;
	if (0 != epoll_ctl(w->epoll_fd, EPOLL_CTL_ADD, fd, &ev)) {
		error = errno;
		return (error);
	}
	error = mcw_worker_reg_add(w, fd);
	if (0 != error)
		return (error);
	/* Record in pool map. */
	pthread_mutex_lock(&pool->map_lock);
	if (pool->map_cnt == pool->map_cap) {
		size_t nc = ((0 == pool->map_cap) ? 64 :
		    (pool->map_cap * 2));
		struct mcw_map_e *nm = realloc(pool->map,
		    (sizeof(struct mcw_map_e) * nc));

		if (NULL == nm) {
			pthread_mutex_unlock(&pool->map_lock);
			epoll_ctl(w->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
			mcw_worker_reg_del(w, fd);
			return (ENOMEM);
		}
		pool->map = nm;
		pool->map_cap = nc;
	}
	pool->map[pool->map_cnt].fd = fd;
	pool->map[pool->map_cnt].widx = widx;
	pool->map[pool->map_cnt].owned = owned;
	pool->map_cnt ++;
	pthread_mutex_unlock(&pool->map_lock);
	return (0);
}

int
mcw_socket_add_to(mcw_pool_t *pool, int worker_idx, int fd) {
	if (NULL == pool || 0 > worker_idx ||
	    pool->workers_cnt <= worker_idx || 0 > fd)
		return (EINVAL);
	if (pool->map_cnt >= MCW_MAX_SOCKETS)
		return (E2BIG);
	return (mcw_socket_add_worker(pool, worker_idx, fd, 0));
}

int
mcw_socket_add(mcw_pool_t *pool, int fd, int *worker_idx_ret) {
	int widx;

	if (NULL == pool || 0 > fd)
		return (EINVAL);
	widx = atomic_fetch_add_explicit(&pool->next_rr, 1,
	    memory_order_relaxed) % pool->workers_cnt;
	if (NULL != worker_idx_ret)
		(*worker_idx_ret) = widx;
	return (mcw_socket_add_worker(pool, widx, fd, 0));
}

int
mcw_multicast_close(mcw_pool_t *pool, int fd) {
	if (NULL == pool || 0 > fd)
		return (EINVAL);
	pthread_mutex_lock(&pool->map_lock);
	for (size_t k = 0; k < pool->map_cnt; k ++) {
		if (pool->map[k].fd == fd) {
			if (0 == pool->map[k].owned) {
				pthread_mutex_unlock(&pool->map_lock);
				return (EACCES); /* Not pool-owned. */
			}
			epoll_ctl(pool->workers[pool->map[k].widx].epoll_fd,
			    EPOLL_CTL_DEL, fd, NULL);
			mcw_worker_reg_del(
			    &pool->workers[pool->map[k].widx], fd);
			close(fd);
			pool->map[k] = pool->map[-- pool->map_cnt];
			pthread_mutex_unlock(&pool->map_lock);
			return (0);
		}
	}
	pthread_mutex_unlock(&pool->map_lock);
	return (ENOENT);
}/*
 * Open a multicast socket: create (NONBLOCK|CLOEXEC), SO_REUSEADDR +
 * SO_REUSEPORT, bind INADDR_ANY:port, enable IP_PKTINFO, join the
 * group on interface if_index (IP_ADD_MEMBERSHIP via ip_mreqn -
 * the "imr_interface" step), register on a round-robin worker.
 * The socket is pool-owned: closed on mcw_pool_destroy().
 */
int
mcw_multicast_open(mcw_pool_t *pool, const char *group_ip,
    uint16_t port, unsigned int if_index,
    int *fd_ret, int *worker_idx_ret) {
	struct sockaddr_in sa;
	struct ip_mreqn mreq;
	uint8_t group_bin[4];
	int fd, on = 1, error, widx;

	if (NULL == pool || NULL == group_ip || 0 == port)
		return (EINVAL);
	if (1 != inet_pton(AF_INET, group_ip, group_bin))
		return (EINVAL);

	fd = socket(AF_INET, (SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC),
	    IPPROTO_UDP);
	if (-1 == fd)
		return (errno);
	/* Allow multiple receivers on the same group:port. */
	if (0 != setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
		error = errno;
		goto err_out;
	}
	if (0 != setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on))) {
		error = errno;
		goto err_out;
	}
	memset(&sa, 0x00, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_ANY);
	sa.sin_port = htons(port);
	if (0 != bind(fd, (const struct sockaddr*)&sa, sizeof(sa))) {
		error = errno;
		goto err_out;
	}
	/* Enable IP_PKTINFO: the worker reports the incoming interface. */
	if (0 != setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &on, sizeof(on))) {
		error = errno;
		goto err_out;
	}
	/* Join the group on the given interface (ip_mreqn, index-based). */
	memset(&mreq, 0x00, sizeof(mreq));
	memcpy(&mreq.imr_multiaddr, group_bin, sizeof(group_bin));
	mreq.imr_address.s_addr = htonl(INADDR_ANY);
	mreq.imr_ifindex = (int)if_index;
	if (0 != setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
	    &mreq, sizeof(mreq))) {
		error = errno;
		goto err_out;
	}
	/* Default multicast interface for outgoing (non-fatal). */
	if (0 != setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF,
	    &mreq, sizeof(mreq))) {
		/* Ignored: receiving does not depend on it. */
	}
	error = mcw_socket_add(pool, fd, &widx);
	if (0 != error)
		goto err_out;
	/* Mark as pool-owned so destroy() closes it. */
	pthread_mutex_lock(&pool->map_lock);
	for (size_t k = 0; k < pool->map_cnt; k ++) {
		if (pool->map[k].fd == fd) {
			pool->map[k].owned = 1;
			break;
		}
	}
	pthread_mutex_unlock(&pool->map_lock);
	if (NULL != fd_ret)
		(*fd_ret) = fd;
	if (NULL != worker_idx_ret)
		(*worker_idx_ret) = widx;
	return (0);
err_out:
	close(fd);
	return (error);
}

void
mcw_pool_stats_get(const mcw_pool_t *pool, mcw_stats_t *st_ret) {
	if (NULL == pool || NULL == st_ret)
		return;
	st_ret->packets = atomic_load_explicit(&pool->stats.packets,
	    memory_order_relaxed);
	st_ret->bytes = atomic_load_explicit(&pool->stats.bytes,
	    memory_order_relaxed);
	st_ret->errors = atomic_load_explicit(&pool->stats.errors,
	    memory_order_relaxed);
	st_ret->epolls = atomic_load_explicit(&pool->stats.epolls,
	    memory_order_relaxed);
	for (int i = 0; i < pool->workers_cnt; i ++) {
		const mcw_stats_t *ws = &pool->workers[i].stats;

		st_ret->packets += atomic_load_explicit(&ws->packets,
		    memory_order_relaxed);
		st_ret->bytes += atomic_load_explicit(&ws->bytes,
		    memory_order_relaxed);
		st_ret->errors += atomic_load_explicit(&ws->errors,
		    memory_order_relaxed);
		st_ret->epolls += atomic_load_explicit(&ws->epolls,
		    memory_order_relaxed);
	}
}

int
mcw_worker_sockets(const mcw_pool_t *pool, int worker_idx) {
	if (NULL == pool || 0 > worker_idx ||
	    worker_idx >= pool->workers_cnt)
		return (-1);
	return (pool->workers[worker_idx].fd_cnt);
}