/*-
 * Copyright (c) 2016-2026 Rozhuk Ivan <rozhuk.im@gmail.com>
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
 * Implements the missing parts of the DVB support in msd:
 *  - demux PID filters (DMX_SET_PES_FILTER, DMX_START), see Astra
 *    modules/dvb/input.c: dmx_set_pid();
 *  - TS output read from the DVR device, see Astra dvr_open();
 *  - configuration XML parsing for adapter/frontend/demux/frequency/etc;
 *  - frontend tuning is done by dvb_fe.c (DVBv5 S2API + DVBv3 fallback).
 */

#include <sys/param.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <syslog.h>
#include <inttypes.h>
#include <arpa/inet.h> /* ntohs() */

#include <linux/dvb/version.h>
#include <linux/dvb/frontend.h>
#include <linux/dvb/dmx.h>

#include "utils/macro.h"
#include "threadpool/threadpool_task.h"
#include "utils/mem_utils.h"
#include "utils/str2num.h"
#include "utils/xml.h"

#include "src_dvb.h"


/* Text names -> DVB enum values. */
typedef struct dvb_str2val_s {
	const char	*name;
	uint32_t	val;
} dvb_str2val_t;

/* Parse value: try text name first, then, if allow_num, plain number. */
static uint32_t
dvb_str2val(const dvb_str2val_t *tbl, const uint8_t *data, size_t data_size,
    int allow_num) {
	uint32_t val = 0;
	size_t i;

	if (NULL == tbl || NULL == data || 0 == data_size)
		return (0);
	/* Text first: avoids ambiguity like "8" (MHz) vs enum value. */
	for (; NULL != tbl->name; tbl ++) {
		/* NOTE: mem_cmpin_cstr() computes sizeof(literal), so for a
		 * runtime name pointer we must use mem_cmpin() + strlen(). */
		if (data_size == strlen(tbl->name) &&
		    0 == mem_cmpin(tbl->name, strlen(tbl->name),
		    data, data_size))
			return (tbl->val);
	}
	if (0 == allow_num)
		return (0);
	/* Plain number. */
	for (i = 0; i < data_size; i ++) {
		if ('0' > data[i] || '9' < data[i])
			return (0); /* Not a number. */
		val = ((val * 10) + (uint32_t)(data[i] - '0'));
	}
	return (val);
}

static const dvb_str2val_t dvb_delsys_names[] = {
	{ "DVBS",			SYS_DVBS },
	{ "DVBS2",			SYS_DVBS2 },
	{ "TURBO",			SYS_TURBO },
	{ "DVBT",			SYS_DVBT },
	{ "DVBT2",			SYS_DVBT2 },
	{ "DVBC",			SYS_DVBC_ANNEX_A },
	{ "DVBC_ANNEX_A",		SYS_DVBC_ANNEX_A },
	{ "DVBC_ANNEX_B",		SYS_DVBC_ANNEX_B },
	{ "DVBC_ANNEX_C",		SYS_DVBC_ANNEX_C },
	{ "ATSC",			SYS_ATSC },
	{ "ATSCMH",			SYS_ATSCMH },
	{ "ISDBT",			SYS_ISDBT },
	{ "ISDBS",			SYS_ISDBS },
	{ "DTMB",			SYS_DTMB },
	{ NULL, 0 }
};

static const dvb_str2val_t dvb_modulation_names[] = {
	{ "QPSK",			QPSK },
	{ "DQPSK",			DQPSK },
	{ "8PSK",			PSK_8 },
	{ "16APSK",			APSK_16 },
	{ "32APSK",			APSK_32 },
	{ "QAM16",			QAM_16 },
	{ "QAM32",			QAM_32 },
	{ "QAM64",			QAM_64 },
	{ "QAM128",			QAM_128 },
	{ "QAM256",			QAM_256 },
	{ "QAMAUTO",			QAM_AUTO },
	{ "VSB8",			VSB_8 },
	{ "VSB16",			VSB_16 },
	{ NULL, 0 }
};

static const dvb_str2val_t dvb_fec_names[] = {
	{ "NONE",			FEC_NONE },
	{ "AUTO",			FEC_AUTO },
	{ "1_2",			FEC_1_2 },
	{ "2_3",			FEC_2_3 },
	{ "3_4",			FEC_3_4 },
	{ "4_5",			FEC_4_5 },
	{ "5_6",			FEC_5_6 },
	{ "6_7",			FEC_6_7 },
	{ "7_8",			FEC_7_8 },
	{ "8_9",			FEC_8_9 },
	{ "3_5",			FEC_3_5 },
	{ "9_10",			FEC_9_10 },
	{ "2_5",			FEC_2_5 },
	{ NULL, 0 }
};

static const dvb_str2val_t dvb_inversion_names[] = {
	{ "OFF",			INVERSION_OFF },
	{ "ON",				INVERSION_ON },
	{ "AUTO",			INVERSION_AUTO },
	{ NULL, 0 }
};

static const dvb_str2val_t dvb_rolloff_names[] = {
	{ "0.35",			ROLLOFF_35 },
	{ "0.20",			ROLLOFF_20 },
	{ "0.25",			ROLLOFF_25 },
	{ "AUTO",			ROLLOFF_AUTO },
	{ NULL, 0 }
};

/* Bandwidth is text only: plain numbers conflict with the enum values. */
static const dvb_str2val_t dvb_bandwidth_names[] = {
	{ "AUTO",			BANDWIDTH_AUTO },
	{ "BANDWIDTH_AUTO",		BANDWIDTH_AUTO },
	{ "1.712MHZ",			BANDWIDTH_1_712_MHZ },
	{ "5MHZ",			BANDWIDTH_5_MHZ },
	{ "6MHZ",			BANDWIDTH_6_MHZ },
	{ "7MHZ",			BANDWIDTH_7_MHZ },
	{ "8MHZ",			BANDWIDTH_8_MHZ },
	{ "10MHZ",			BANDWIDTH_10_MHZ },
	{ NULL, 0 }
};


void
src_dvb_conn_def(str_src_conn_dvb_p s_ret) {

	if (NULL == s_ret)
		return;
	memset(s_ret, 0x00, sizeof(str_src_conn_dvb_t));
	s_ret->dmx_buf_size = SRC_DVB_DEF_DMX_BUF;
	s_ret->dvr_buf_size = SRC_DVB_DEF_DVR_BUF;
	s_ret->delivery_sys = SYS_UNDEFINED;
	s_ret->modulation = QAM_AUTO;
	s_ret->fec = FEC_AUTO;
	s_ret->spec_inv = INVERSION_AUTO;
	s_ret->rolloff = ROLLOFF_AUTO;
	s_ret->bandwidth = BANDWIDTH_AUTO;
	s_ret->stream_id = NO_STREAM_ID_FILTER;
}

int
src_dvb_conn_xml_load(const uint8_t *buf, size_t buf_size,
    str_src_conn_dvb_p s) {
	const uint8_t *data, *ptm, *cur_pos;
	size_t data_size, tm;
	uint32_t tm32;

	if (NULL == buf || 0 == buf_size || NULL == s)
		return (EINVAL);

	/* DVB settings section (optional: keep defaults if absent). */
	if (0 != xml_get_val_args(buf, buf_size, NULL, NULL, NULL,
	    &data, &data_size, (const uint8_t*)"dvb", NULL))
		return (0);

	xml_get_val_uint32_args(data, data_size, NULL, &s->adapter_idx,
	    (const uint8_t*)"adapter", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->fe_idx,
	    (const uint8_t*)"frontend", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->dmx_idx,
	    (const uint8_t*)"demux", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->dmx_buf_size,
	    (const uint8_t*)"dmxBufSize", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->dvr_buf_size,
	    (const uint8_t*)"dvrBufSize", NULL);

	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"deliverySystem", NULL)) {
		tm32 = dvb_str2val(dvb_delsys_names, ptm, tm, 1);
		if (0 != tm32)
			s->delivery_sys = tm32;
	}
	xml_get_val_uint32_args(data, data_size, NULL, &s->frequency,
	    (const uint8_t*)"frequency", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->symbol_rate,
	    (const uint8_t*)"symbolRate", NULL);
	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"modulation", NULL)) {
		tm32 = dvb_str2val(dvb_modulation_names, ptm, tm, 1);
		if (0 != tm32)
			s->modulation = tm32;
	}
	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"fec", NULL)) {
		tm32 = dvb_str2val(dvb_fec_names, ptm, tm, 1);
		if (0 != tm32)
			s->fec = tm32;
	}
	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"inversion", NULL)) {
		tm32 = dvb_str2val(dvb_inversion_names, ptm, tm, 1);
		if (0 != tm32)
			s->spec_inv = tm32;
	}
	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"rolloff", NULL)) {
		tm32 = dvb_str2val(dvb_rolloff_names, ptm, tm, 1);
		if (0 != tm32)
			s->rolloff = tm32;
	}
	if (0 == xml_get_val_args(data, data_size, NULL, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"bandwidth", NULL)) {
		tm32 = dvb_str2val(dvb_bandwidth_names, ptm, tm, 0);
		if (0 != tm32)
			s->bandwidth = tm32;
	}
	xml_get_val_uint32_args(data, data_size, NULL, &s->stream_id,
	    (const uint8_t*)"streamId", NULL);
	xml_get_val_uint32_args(data, data_size, NULL, &s->pnr,
	    (const uint8_t*)"pnr", NULL);

	/* Optional PID list (empty = all PIDs). */
	s->pids_count = 0;
	cur_pos = NULL;
	while ((STR_SRC_DVB_PIDS_MAX - 1) > s->pids_count &&
	    0 == xml_get_val_args(data, data_size, &cur_pos, NULL, NULL,
	    &ptm, &tm, (const uint8_t*)"pidsList", "pid", NULL)) {
		if (0 == tm)
			continue; /* Skip empty. */
		s->pids[s->pids_count ++] = (uint16_t)ustr2usize(ptm, tm);
	}
	/* The MPEG-TS analyzer needs PAT (PID 0): always subscribe it. */
	for (tm = 0; tm < s->pids_count; tm ++) {
		if (0 == s->pids[tm])
			break;
	}
	if (tm == s->pids_count && STR_SRC_DVB_PIDS_MAX > s->pids_count)
		s->pids[s->pids_count ++] = 0;

	return (0);
}


static int
src_dvb_dvr_open(src_dvb_p src_dvb) {
	char dev_name[64];

	if (NULL == src_dvb)
		return (EINVAL);
	if (0 <= src_dvb->dvr_fd) { /* Close previous descriptor. */
		close(src_dvb->dvr_fd);
		src_dvb->dvr_fd = -1;
	}
	snprintf(dev_name, sizeof(dev_name),
	    "/dev/dvb/adapter%"PRIu32"/dvr%"PRIu32,
	    src_dvb->s.adapter_idx, src_dvb->s.dmx_idx);
	src_dvb->dvr_fd = open(dev_name, (O_RDONLY | O_NONBLOCK));
	if (0 > src_dvb->dvr_fd) {
		src_dvb->dvr_fd = -1;
		return (errno);
	}
	return (0);
}

/* Subscribe/unsubscribe TS PID on the demux device. */
int
src_dvb_dmx_pid_set(src_dvb_p src_dvb, uint16_t pid, int set) {
	size_t i;
	int fd;
	char dev_name[64];
	struct dmx_pes_filter_params pes;
	int error;

	if (NULL == src_dvb)
		return (EINVAL);
	if (0 == set) { /* Unsubscribe. */
		for (i = 0; i < src_dvb->dmx_cnt; i ++) {
			if (pid != src_dvb->dmx_pid[i])
				continue;
			close(src_dvb->dmx_fd[i]);
			src_dvb->dmx_cnt --;
			src_dvb->dmx_fd[i] = src_dvb->dmx_fd[src_dvb->dmx_cnt];
			src_dvb->dmx_pid[i] = src_dvb->dmx_pid[src_dvb->dmx_cnt];
			break;
		}
		return (0);
	}
	/* Already subscribed. */
	for (i = 0; i < src_dvb->dmx_cnt; i ++) {
		if (pid == src_dvb->dmx_pid[i])
			return (0);
	}
	if (SRC_DVB_DMX_MAX_FDS <= src_dvb->dmx_cnt)
		return (ENOSPC);

	snprintf(dev_name, sizeof(dev_name),
	    "/dev/dvb/adapter%"PRIu32"/demux%"PRIu32,
	    src_dvb->s.adapter_idx, src_dvb->s.dmx_idx);
	fd = open(dev_name, (O_RDWR | O_NONBLOCK));
	if (0 > fd)
		return (errno);
	/* Set demux buffer size (kB -> bytes). */
	if (0 != src_dvb->dmx_buf_size) {
		uint32_t tmv = (src_dvb->dmx_buf_size * 1024);
		if (0 != ioctl(fd, DMX_SET_BUFFER_SIZE, tmv)) {
			error = errno;
			SYSLOG_ERR(LOG_ERR, error,
			    "Tuner %"PRIu32", frontend %"PRIu32" - "
			    "ioctl(DMX_SET_BUFFER_SIZE) fail.",
			    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
			close(fd);
			return (error);
		}
	}
	/* PID filter: output TS packets of this PID to the demux tap (DVR). */
	memset(&pes, 0x00, sizeof(pes));
	pes.pid = pid;
	pes.input = DMX_IN_FRONTEND;
	pes.output = DMX_OUT_TSDEMUX_TAP;
	pes.pes_type = DMX_PES_OTHER;
	pes.flags = DMX_IMMEDIATE_START;
	if (0 != ioctl(fd, DMX_SET_PES_FILTER, &pes)) {
		error = errno;
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - "
		    "ioctl(DMX_SET_PES_FILTER, pid=%u) fail.",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx, pid);
		close(fd);
		return (error);
	}
	if (0 != ioctl(fd, DMX_START)) {
		error = errno;
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - ioctl(DMX_START) fail.",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		close(fd);
		return (error);
	}
	src_dvb->dmx_fd[src_dvb->dmx_cnt] = fd;
	src_dvb->dmx_pid[src_dvb->dmx_cnt] = pid;
	src_dvb->dmx_cnt ++;
	return (0);
}

int
src_dvb_fe_status_get(src_dvb_p src_dvb, fe_status_t *status_ret) {

	if (NULL == src_dvb || NULL == status_ret)
		return (EINVAL);
	(*status_ret) = src_dvb->fe_status;
	return (0);
}


static int
src_dvb_fe_on_state_cb(dvb_fe_p dvb_fe, void *udata, const dvb_fe_state_p state) {
	src_dvb_p src_dvb = udata;

	if (NULL == src_dvb || NULL == state)
		return (0);
	src_dvb->fe_status = state->status;
	if (0 != (FE_HAS_LOCK & state->status)) {
		if (0 == src_dvb->fe_lock) {
			src_dvb->fe_lock = 1;
			syslog(LOG_INFO,
			    "Tuner %"PRIu32", frontend %"PRIu32": FE_HAS_LOCK.",
			    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		}
	} else {
		if (0 != src_dvb->fe_lock) {
			src_dvb->fe_lock = 0;
			syslog(LOG_INFO,
			    "Tuner %"PRIu32", frontend %"PRIu32": FE lock lost.",
			    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		}
	}
	if (NULL != src_dvb->on_state) {
		return (src_dvb->on_state(src_dvb, src_dvb->udata,
		    (uint32_t)state->status));
	}
	return (0);
}

int
src_dvb_create(const str_src_conn_dvb_p s, tpt_p tpt,
    src_dvb_on_state_cb on_state, void *udata, src_dvb_p *src_dvb_ret) {
	int error;
	src_dvb_p src_dvb;
	dvb_fe_settings_t fe_s;

	if (NULL == s || NULL == tpt || NULL == src_dvb_ret)
		return (EINVAL);
	if (SYS_UNDEFINED == s->delivery_sys) {
		syslog(LOG_ERR,
		    "Tuner %"PRIu32", frontend %"PRIu32": "
		    "dvb.deliverySystem is not set in the channel config.",
		    s->adapter_idx, s->fe_idx);
		return (EINVAL);
	}
	src_dvb = calloc(1, sizeof(src_dvb_t));
	if (NULL == src_dvb)
		return (ENOMEM);
	src_dvb->tpt = tpt;
	src_dvb->dvr_fd = -1;
	src_dvb->on_state = on_state;
	src_dvb->udata = udata;
	memcpy(&src_dvb->s, s, sizeof(str_src_conn_dvb_t));

	/* Create and configure frontend object. */
	error = dvb_fe_create(s->adapter_idx, s->fe_idx, tpt,
	    src_dvb_fe_on_state_cb, src_dvb, &src_dvb->dvb_fe);
	if (0 != error) {
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - dvb_fe_create().",
		    s->adapter_idx, s->fe_idx);
		goto err_out;
	}
	dvb_fe_settings_def(&fe_s);
	fe_s.delivery_sys = (fe_delivery_system_t)s->delivery_sys;
	fe_s.frequency = s->frequency;
	fe_s.symbol_rate = s->symbol_rate;
	fe_s.modulation = (fe_modulation_t)s->modulation;
	fe_s.fec = (fe_code_rate_t)s->fec;
	fe_s.spec_inv = (fe_spectral_inversion_t)s->spec_inv;
	fe_s.rolloff = (fe_rolloff_t)s->rolloff;
	fe_s.bandwidth = (fe_bandwidth_t)s->bandwidth;
	fe_s.stream_id = s->stream_id;
	error = dvb_fe_set_settings(src_dvb->dvb_fe, &fe_s);
	if (0 != error) {
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - "
		    "dvb_fe_set_settings().",
		    s->adapter_idx, s->fe_idx);
		goto err_out;
	}

	(*src_dvb_ret) = src_dvb;
	return (0);
err_out:
	src_dvb_destroy(src_dvb);
	return (error);
}

int
src_dvb_start(src_dvb_p src_dvb, uintptr_t *dvr_fd_ret) {
	int error;
	size_t i;

	if (NULL == src_dvb || NULL == dvr_fd_ret)
		return (EINVAL);
	if (NULL == src_dvb->dvb_fe)
		return (EINVAL);

	/* Tune frontend (open device + DVBv5/DVBv3 tune). */
	error = dvb_fe_start(src_dvb->dvb_fe);
	if (0 != error) {
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - dvb_fe_start().",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		return (error);
	}
	/* Open DVR device (TS output). */
	error = src_dvb_dvr_open(src_dvb);
	if (0 != error) {
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - "
		    "src_dvb_dvr_open().",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		goto err_out;
	}
	/* Install demux PID filters.
	 * PNR mode: start with PAT only, PMT and ES PIDs are subscribed
	 * dynamically by src_dvb_pnr_sync() as the analyzer discovers them. */
	if (0 != src_dvb->s.pnr) {
		error = src_dvb_dmx_pid_set(src_dvb, MPEG2_TS_PID_PAT, 1);
	} else if (0 == src_dvb->s.pids_count) { /* All PIDs. */
		error = src_dvb_dmx_pid_set(src_dvb, SRC_DVB_PID_ALL, 1);
	} else {
		for (i = 0; i < src_dvb->s.pids_count; i ++) {
			error = src_dvb_dmx_pid_set(src_dvb,
			    src_dvb->s.pids[i], 1);
			if (0 != error)
				break;
		}
	}
	if (0 != error) {
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32" - "
		    "src_dvb_dmx_pid_set().",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx);
		goto err_out;
	}
	(*dvr_fd_ret) = (uintptr_t)src_dvb->dvr_fd;
	return (0);
err_out:
	src_dvb_stop(src_dvb);
	return (error);
}

void
src_dvb_stop(src_dvb_p src_dvb) {
	size_t i;

	if (NULL == src_dvb)
		return;
	/* Close PID filters (demux). */
	for (i = 0; i < src_dvb->dmx_cnt; i ++) {
		close(src_dvb->dmx_fd[i]);
		src_dvb->dmx_fd[i] = -1;
	}
	src_dvb->dmx_cnt = 0;
	/* Close DVR. */
	if (0 <= src_dvb->dvr_fd) {
		close(src_dvb->dvr_fd);
		src_dvb->dvr_fd = -1;
	}
	/* Stop frontend. */
	if (NULL != src_dvb->dvb_fe)
		dvb_fe_stop(src_dvb->dvb_fe);
	src_dvb->fe_lock = 0;
	src_dvb->pmt_pid = 0;
	src_dvb->es_pids_cnt = 0;
}

void
src_dvb_destroy(src_dvb_p src_dvb) {

	if (NULL == src_dvb)
		return;
	src_dvb_stop(src_dvb);
	if (NULL != src_dvb->dvb_fe) {
		dvb_fe_destroy(src_dvb->dvb_fe);
		src_dvb->dvb_fe = NULL;
	}
	free(src_dvb);
}

int
src_dvb_restart(src_dvb_p src_dvb, uintptr_t *dvr_fd_ret) {

	if (NULL == src_dvb || NULL == dvr_fd_ret)
		return (EINVAL);
	src_dvb_stop(src_dvb);
	return (src_dvb_start(src_dvb, dvr_fd_ret));
}

/* PNR (program number) selection:
 * Discover PMT and elementary stream PIDs for the configured program and
 * subscribe/unsubscribe them on the demux device. Called after each DVR
 * read with the mpeg2ts analyzer state (PAT/PMT results). */
int
src_dvb_pnr_sync(src_dvb_p src_dvb, mpeg2_ts_data_p m2ts) {
	size_t i, j, desired_cnt = 0;
	uint16_t desired[SRC_DVB_DMX_MAX_FDS];
	uint16_t pid;
	mpeg2_ts_prog_p prog = NULL;
	int found, error;

	if (NULL == src_dvb || NULL == m2ts)
		return (EINVAL);
	if (0 == src_dvb->s.pnr) /* PNR mode disabled. */
		return (0);

	/* Find the program in the analyzer's PAT/PMT results. */
	for (i = 0; i < m2ts->prog_cnt; i ++) {
		if ((uint32_t)ntohs((uint16_t)m2ts->progs[i].pn) == src_dvb->s.pnr) {
			prog = &m2ts->progs[i];
			break;
		}
	}
	if (NULL == prog) /* PAT not seen yet. */
		return (0);

	/* Desired PID set: PAT + PMT + program elementary PIDs. */
	desired[desired_cnt ++] = MPEG2_TS_PID_PAT;
	desired[desired_cnt ++] = prog->pmt.pid;
	for (i = 0; i < prog->pids_cnt &&
	    (SRC_DVB_DMX_MAX_FDS - 1) > desired_cnt; i ++) {
		found = 0;
		for (j = 0; j < desired_cnt; j ++) {
			if (desired[j] == prog->pids[i]) {
				found = 1;
				break;
			}
		}
		if (0 == found)
			desired[desired_cnt ++] = prog->pids[i];
	}

	/* Remember discovered state (diagnostics / future /stat). */
	src_dvb->pmt_pid = prog->pmt.pid;
	src_dvb->es_pids_cnt = 0;
	for (i = 2; i < desired_cnt &&
	    (sizeof(src_dvb->es_pids) / sizeof(src_dvb->es_pids[0])) >
	    src_dvb->es_pids_cnt; i ++) {
		src_dvb->es_pids[src_dvb->es_pids_cnt ++] = desired[i];
	}

	/* Subscribe PIDs that are missing. */
	for (i = 0; i < desired_cnt; i ++) {
		found = 0;
		for (j = 0; j < src_dvb->dmx_cnt; j ++) {
			if (desired[i] == src_dvb->dmx_pid[j]) {
				found = 1;
				break;
			}
		}
		if (0 != found)
			continue;
		error = src_dvb_dmx_pid_set(src_dvb, desired[i], 1);
		SYSLOG_ERR(LOG_ERR, error,
		    "Tuner %"PRIu32", frontend %"PRIu32": "
		    "src_dvb_dmx_pid_set(%"PRIu16") fail.",
		    src_dvb->s.adapter_idx, src_dvb->s.fe_idx, desired[i]);
	}

	/* Unsubscribe PIDs that are no longer desired.
	 * Note: src_dvb_dmx_pid_set(pid, 0) compacts the arrays, so do not
	 * advance i after a removal. PAT is always kept. */
	for (i = 0; i < src_dvb->dmx_cnt;) {
		pid = src_dvb->dmx_pid[i];
		if (MPEG2_TS_PID_PAT == pid) {
			i ++;
			continue;
		}
		found = 0;
		for (j = 0; j < desired_cnt; j ++) {
			if (desired[j] == pid) {
				found = 1;
				break;
			}
		}
		if (0 != found) {
			i ++;
			continue;
		}
		src_dvb_dmx_pid_set(src_dvb, pid, 0);
	}
	return (0);
}