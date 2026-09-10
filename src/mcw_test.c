/* Smoke test for multicast_worker module. */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/time.h>
#include "multicast_worker.h"

static _Atomic uint64_t pkts;
static _Atomic uint64_t bytes;
static _Atomic uint64_t ifs;

static void
on_packet(void *udata, const uint8_t *data, size_t size,
    const struct sockaddr_storage *src, int if_index) {
	(void)udata; (void)data; (void)src;
	atomic_fetch_add(&pkts, 1);
	atomic_fetch_add(&bytes, size);
	if (0 != if_index)
		atomic_fetch_add(&ifs, 1);
}

int
main(void) {
	mcw_pool_t *pool = NULL;
	mcw_stats_t st;

	struct sockaddr_in sa;
	int cpus[4] = { 0, 1, 2, 3 };
	int error = 0;
	int fds[2];

	error = mcw_pool_create(&pool, 4, cpus, on_packet, NULL);
	if (0 != error) { printf("create FAIL %i\n", error); return (1); }
	error = mcw_pool_start(pool);
	if (0 != error) { printf("start FAIL %i\n", error); return (1); }

	/* Unicast socket bound to localhost. */
	fds[0] = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sa.sin_port = htons(19555);
	if (0 != bind(fds[0], (struct sockaddr*)&sa, sizeof(sa))) {
		printf("bind FAIL\n"); return (1);
	}
	if (0 != mcw_socket_add(pool, fds[0], NULL)) {
		printf("socket_add FAIL\n"); return (1);
	}
	/* Multicast socket on loopback (join test). */
	{
		int mfd = -1, widx = -1;

		error = mcw_multicast_open(pool, "239.255.0.1", 19600,
		    1 /* lo */, &mfd, &widx);
		printf("multicast_open: %i (fd=%i, worker=%i)\n",
		    error, mfd, widx);
	}
	/* Sender. */
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sa.sin_port = htons(19555);
	fds[1] = socket(AF_INET, SOCK_DGRAM, 0);
	for (int i = 0; i < 1000; i ++) {
		uint8_t buf[256];

		memset(buf, (uint8_t)i, sizeof(buf));
		sendto(fds[1], buf, sizeof(buf), 0,
		    (struct sockaddr*)&sa, sizeof(sa));
	}
	usleep(500000);
	{

		mcw_pool_stats_get(pool, &st);
		printf("stats: packets=%llu bytes=%llu errors=%llu "
		    "(cb: %llu pkts, %llu bytes, ifs=%llu)\n",
		    (unsigned long long)st.packets,
		    (unsigned long long)st.bytes,
		    (unsigned long long)st.errors,
		    (unsigned long long)atomic_load(&pkts),
		    (unsigned long long)atomic_load(&bytes),
		    (unsigned long long)atomic_load(&ifs));
		if (1000 != atomic_load(&pkts) ||
		    256000 != atomic_load(&bytes)) {
			printf("TEST FAIL\n");
			mcw_pool_destroy(pool);
			return (1);
		}
	}
	mcw_pool_stop(pool);
	usleep(100000);
	mcw_pool_destroy(pool);
	printf("TEST PASS\n");
	return (0);
}