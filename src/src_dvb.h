/*-
 * Copyright (c) 2016 - 2026 Rozhuk Ivan <rozhuk.im@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * Author: Rozhuk Ivan <rozhuk.im@gmail.com>
 *
 */

/* DVB (digital TV tuner) stream source.
 *
 * Design is based on the DVB module of Astra
 * (https://github.com/cesbo/astra, GPL-3.0):
 *  - frontend is tuned via DVBv5 S2API (with DVBv3 fallback), see dvb_fe.c;
 *  - a single PID filter (0x2000 = all PIDs) or per-PID filters are
 *    installed on the demux device (DMX_SET_PES_FILTER);
 *  - MPEG-TS stream is read from the DVR device (/dev/dvb/adapterX/dvrY).
 */

#ifndef __SRC_DVB_H__
#define __SRC_DVB_H__

#include <sys/param.h>
#include <sys/types.h>

#include <linux/dvb/frontend.h>
#include <linux/dvb/dmx.h>

#include "utils/macro.h"
#include "threadpool/threadpool_task.h"
#include "dvb_fe.h"
#include "stream_src.h"
#include "stream_mpeg2ts.h"


typedef struct src_dvb_s	*src_dvb_p;

/* State and/or Status changed. status - fe_status_t bits. */
typedef int (*src_dvb_on_state_cb)(src_dvb_p src, void *udata, uint32_t status);
/* Ret value: 0: OK, non zero - stop processing, src possible destroyed. */

/* Max number of simultaneous demux PID filters. */
#define SRC_DVB_DMX_MAX_FDS	64
/* Default DMX_SET_BUFFER_SIZE in kB. */
#define SRC_DVB_DEF_DMX_BUF	(2048)
/* Default DVR read buffer size in kB. */
#define SRC_DVB_DEF_DVR_BUF	(256)
/* Special PID value: all PIDs. */
#define SRC_DVB_PID_ALL		0x2000


typedef struct src_dvb_s {
	dvb_fe_p	dvb_fe;		/* DVB frontend. */
	uint16_t	nit_pid;	/* NIT PID (informational). */
	tpt_p		tpt;		/* Thread data for all IO operations. */
	/* Demux PID filters. */
	int		dmx_fd[SRC_DVB_DMX_MAX_FDS];
	uint16_t	dmx_pid[SRC_DVB_DMX_MAX_FDS];
	size_t		dmx_cnt;
	uint32_t	dmx_buf_size;	/* kB. */
	/* DVR (TS output) device. */
	int		dvr_fd;
	uint32_t	dvr_buf_size;	/* kB. */
	/* Frontend status (updated from FE events). */
	fe_status_t	fe_status;
	uint32_t	fe_lock;
	/* PNR (program number) selection state. */
	uint16_t	pmt_pid;	/* Discovered PMT PID for the selected program. */
	uint16_t	es_pids[SRC_DVB_DMX_MAX_FDS]; /* Discovered ES PIDs. */
	uint16_t	es_pids_cnt;
	/* Settings. */
	str_src_conn_dvb_t s;
	src_dvb_on_state_cb on_state;	/* External status notifier. */
	void		*udata;
} src_dvb_t;


/* DVB connection (source) settings. */
void	src_dvb_conn_def(str_src_conn_dvb_p s_ret);
int	src_dvb_conn_xml_load(const uint8_t *buf, size_t buf_size,
	    str_src_conn_dvb_p s);

int	src_dvb_create(const str_src_conn_dvb_p s, tpt_p tpt,
	    src_dvb_on_state_cb on_state, void *udata, src_dvb_p *src_dvb_ret);
void	src_dvb_destroy(src_dvb_p src_dvb);
int	src_dvb_start(src_dvb_p src_dvb, uintptr_t *dvr_fd_ret);
void	src_dvb_stop(src_dvb_p src_dvb);
int	src_dvb_restart(src_dvb_p src_dvb, uintptr_t *dvr_fd_ret);

/* Subscribe/unsubscribe TS PID (0x2000 = all PIDs). 0 = OK. */
int	src_dvb_dmx_pid_set(src_dvb_p src_dvb, uint16_t pid, int set);
int	src_dvb_fe_status_get(src_dvb_p src_dvb, fe_status_t *status_ret);

/* PNR mode: after each DVR read, feed the mpeg2ts analyzer result here.
 * Discovers PMT/ES PIDs for the configured program number and subscribes
 * them on the demux device (PAT -> PMT -> ES PIDs walk). 0 = OK. */
int	src_dvb_pnr_sync(src_dvb_p src_dvb, mpeg2_ts_data_p m2ts);


#endif /* __SRC_DVB_H__ */