# msd

[![Build-macOS-latest Actions Status](https://github.com/rozhuk-im/msd/workflows/build-macos-latest/badge.svg)](https://github.com/rozhuk-im/msd/actions)
[![Build-Ubuntu-latest Actions Status](https://github.com/rozhuk-im/msd/workflows/build-ubuntu-latest/badge.svg)](https://github.com/rozhuk-im/msd/actions)


Rozhuk Ivan <rozhuk.im@gmail.com> 2011-2026

msd - Multi stream daemon.
Program for organizing IP TV streaming on the network via HTTP.


## Licence
BSD licence.
Website: http://www.netlab.linkpc.net/wiki/en:software:msd:index


## Donate
Support the author
* **GitHub Sponsors:** [!["GitHub Sponsors"](https://camo.githubusercontent.com/220b7d46014daa72a2ab6b0fcf4b8bf5c4be7289ad4b02f355d5aa8407eb952c/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f2d53706f6e736f722d6661666266633f6c6f676f3d47697448756225323053706f6e736f7273)](https://github.com/sponsors/rozhuk-im) <br/>
* **Buy Me A Coffee:** [!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/rojuc) <br/>
* **PayPal:** [![PayPal](https://srv-cdn.himpfen.io/badges/paypal/paypal-flat.svg)](https://paypal.me/rojuc) <br/>
* **Bitcoin (BTC):** `1AxYyMWek5vhoWWRTWKQpWUqKxyfLarCuz` <br/>


## Features
* support for IPv4 and IPv6
* Zero Copy on Send (ZCoS) - reduces the overhead of service connected clients, all the work of sending the data to the client assumes the OS kernel 
* support half closed http clients
* receiving udp-multicast, including rtp, simultaneously with different interfaces
* the use of various TCP Congestion Control algorithms depending on the port to which the client came and the URL the client's request
* instantaneous sending new client data from the ring buffer in order to minimize waiting times start playback
* sending any additional http headers in requests and responses
* detailed statistics for each TCP connection, to help you find problems at the network level



## Compilation and Installation
```
sudo apt-get install build-essential git cmake fakeroot
git clone --recursive https://github.com/rozhuk-im/msd.git
cd msd
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_VERBOSE_MAKEFILE=true ..
make -j 8
```


## Run tests
```
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=1 ..
cmake --build . --config Release -j 16
ctest -C Release --output-on-failure -j 16
```


## Usage
```
msd [-d] [-v] [-c file]
       [-p PID file] [-u uid|usr -g gid|grp]
 -h           usage (this screen)
 -d           become daemon
 -c file      config file
 -p PID file  file name to store PID
 -u uid|user  change uid
 -g gid|group change gid
 -v           verboce
```


## Setup

### msd
Copy %%ETCDIR%%/msd.conf.sample to %%ETCDIR%%/msd.conf
then replace lan0 with your network interface name.
Add more sections if needed.
Remove IPv4/IPv6 lines if not needed.

Add to /etc/rc.conf:
```
msd_enable="YES"
```

Run:
```
service msd restart
```

## DVB (digital TV tuner) support

Built-in DVB-карт support (Linux only, requires `/dev/dvb/adapter*/frontend*`,
`demux*`, `dvr*` devices). A DVB channel is a regular msd channel whose
source `<type>` is `dvb`:

```xml
<channel>
	<name>dvb-t2.ts</name>
	<hubProfileName>default</hubProfileName>
	<sourceList>
		<source>
			<type>dvb</type>
			<dvb>
				<adapter>0</adapter>
				<frontend>0</frontend>
				<demux>0</demux>
				<dmxBufSize>2048</dmxBufSize>
				<deliverySystem>DVBT2</deliverySystem>
				<frequency>578000000</frequency>
				<bandwidth>8MHZ</bandwidth>
			</dvb>
		</source>
	</sourceList>
</channel>
```

See `conf/msd_channels_dvb.conf` for a complete example and a satellites (DVB-S/S2)
example. Request the channel as any other:
`http://<msd-ip>:<port>/channel/dvb-t2.ts`.

### Выбор программы (PNR)

На одном транспондере обычно несколько телеканалов. Параметр `pnr` задаёт
**номер программы** (Program Number), которую нужно вещать:

```xml
<dvb>
	<deliverySystem>DVBT2</deliverySystem>
	<frequency>578000000</frequency>
	<bandwidth>8MHZ</bandwidth>
	<pnr>1</pnr>   <!-- выбрать программу номер 1 (0 = весь транспондер) -->
</dvb>
```

При `pnr > 0` msd сам находит PIDs канала по PAT/PMT:
1. подписывается на PID 0 (PAT);
2. из PAT находит PMT PID по номеру программы;
3. из PMT получает список ES-PIDs (видео/аудио) и подписывает их на demux.

Так как номера программ на разных транспондерах/провайдерах отличаются,
подберите `pnr` экспериментально (помогает `dvbscan`, или перечислите
доступные программы через `/stat` — msd показывает список
`Programm: <номер> [PID: ...]` для каждого распознанного канала при
`pnr = 0`).

### `<dvb>` keys

| Key | Meaning | Default |
|---|---|---|
| `adapter` | DVB adapter number (`/dev/dvb/adapterX`) | 0 |
| `frontend` | Frontend number (`.../frontendY`) | 0 |
| `demux` | Demux/DVR number (`.../demuxY`, `.../dvrY`) | 0 |
| `dmxBufSize` | Demux buffer size in kB (`DMX_SET_BUFFER_SIZE`) | 2048 |
| `dvrBufSize` | DVR read buffer size in kB (hint) | 256 |
| `deliverySystem` | `DVBT`, `DVBT2`, `DVBS`, `DVBS2`, `DVBC`, `ATSC`, `ISDBT`, or numeric `SYS_*` | required |
| `frequency` | Hz. Satellite: LNB output (IF) frequency | 0 |
| `symbolRate` | Sym/s (satellite/cable) | 0 |
| `modulation` | `QPSK`, `8PSK`, `QAM16`, `QAM64`, `QAM256`, `VSB8`... or numeric | QAM_AUTO |
| `fec` | `1_2`..`9_10`, `AUTO`, or numeric | FEC_AUTO |
| `inversion` | `ON`, `OFF`, `AUTO` | INVERSION_AUTO |
| `rolloff` | `0.35`, `0.20`, `0.25`, `AUTO` | ROLLOFF_AUTO |
| `bandwidth` | Text only: `5MHZ`, `6MHZ`, `7MHZ`, `8MHZ`, `10MHZ`, `AUTO` | BANDWIDTH_AUTO |
| `streamId` | DVB-T2 PLP (`DTV_STREAM_ID`) | all PIDs |
| `pnr` | **Program Number** to select. PIDs are auto-discovered from PAT/PMT (PAT → PMT → ES PIDs are subscribed dynamically on the demux device). `0` = whole transponder | 0 (all PIDs) |
| `pidsList` | `pid` list to filter; empty = all PIDs. **Ignored when `pnr` is set** (PID 0/PAT is auto-subscribed) | all PIDs |

Implementation notes:
* The frontend driver (`dvb_fe.c`) uses DVBv5 S2API with DVBv3 `FE_SET_FRONTEND`
  fallback.
* TS PIDs are filtered on the demux device (Astra-style `DMX_SET_PES_FILTER`),
  the resulting MPEG-TS is read from the DVR device and fed into the regular
  msd ring buffer / MPEG-TS analyzer pipeline.
* FE lock status is reported in `/stat` and in syslog (`FE_HAS_LOCK` / lock lost).
* The only writing part is `src_dvb.c` (demux/DVR/config) - the frontend part
  (`dvb_fe.c`) already existed in the tree. DVB code is compiled only on Linux.

