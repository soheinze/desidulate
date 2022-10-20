#!/usr/bin/python3

# https://hvsc.c64.org/download/C64Music/DOCUMENTS/SID_file_format.txt

import os
import re
import subprocess
import struct
import pandas as pd


def intdecode(_, x):
    return int(x)


def strdecode(_, x):
    return x.decode('latin1').rstrip('\x00')


def sidaddr(_, x):
    x = int(x) << 4
    if x:
        x += 0xd000
    return x


def bitsdecode(x, d):
    return d[x & max(d.keys())]


def sidmodel(x):
    return bitsdecode(x, {0: None, 1: '6581', 2: '8580', 3: '6581+8580'})


def clock(x):
    return bitsdecode(x, {0: 'Unknown', 1: 'PAL', 2: 'NTSC', 3: 'PAL+NTSC'})


def binformat(x):
    return bitsdecode(x, {0: 'built-in', 1: 'MUS'})


def psidspecific(rsid, x):
    if rsid:
        return bitsdecode(x, {0: 'c64', 1: 'basic'})
    else:
        return bitsdecode(x, {0: 'c64', 1: 'psid'})


def decodeflags(rsid, x):
    x = int(x)
    return {
        'binformat': binformat(x),
        'psidSpecific': psidspecific(rsid, x),
        'clock': clock((x >> 2)),
        'sidmodel': sidmodel((x >> 4)),
        'sidmodel2': sidmodel((x >> 6)),
        'sidmodel3': sidmodel((x >> 8)),
    }


def decodespeed(rsid, x):
    x = int(x)
    if not rsid:
        # assume all same speed scheme
        if x:
            return 'CIA'
    return 'VBI'


SID_HEADER_LEN = 0x7C
SID_HEADERS = (
        # +00    STRING magicID: 'PSID' or 'RSID'
        ('magicID', '4s', strdecode),
        # +04    WORD version
        ('version', 'H', intdecode),
        # +06    WORD dataOffset
        ('dataOffset', 'H', intdecode),
        # +08    WORD loadAddress
        ('loadAddress', 'H', intdecode),
        # +0A    WORD initAddress
        ('initAddress', 'H', intdecode),
        # +0C    WORD playAddress
        ('playAddress', 'H', intdecode),
        # +0E    WORD songs
        ('songs', 'H', intdecode),
        # +10    WORD startSong
        ('startSong', 'H', intdecode),
        # +12    LONGWORD speed
        ('speed', 'I', decodespeed),
        # +16    STRING ``<name>''
        ('name', '32s', strdecode),
        # +36    STRING ``<author>''
        ('author', '32s', strdecode),
        # +56    STRING ``<released>''
        ('released', '32s', strdecode),
        # +76    WORD flags
        ('flags', 'H', decodeflags),
        # +78    BYTE startPage (relocStartPage)
        ('startPage', 'B', intdecode),
        # +79    BYTE pageLength (relocPages)
        ('pageLength', 'B', intdecode),
        # +7A    BYTE secondSIDAddress
        ('secondSIDAddress', 'B', intdecode),
        # +7B    BYTE thirdSIDAddress
        ('thirdSIDAddresss', 'B', intdecode),
)


def sidinfo(sidfile):
    with open(sidfile, 'rb') as f:
        data = f.read()[:SID_HEADER_LEN]
    unpack_format = '>' + ''.join((field_type for _, field_type, _ in SID_HEADERS))
    results = struct.unpack(unpack_format, data)
    rsid = results[0] == b'RSID'
    decoded = {'path': sidfile}
    for header_data, field_data in zip(SID_HEADERS, results):
        field, _, decode = header_data
        decoded_field = decode(rsid, field_data)
        if isinstance(decoded_field, dict):
            decoded.update(decoded_field)
        else:
            decoded[field] = decoded_field

    decoded['sids'] = 1
    for sid in ('secondSIDAddress', 'thirdSIDAddresss'):
        if decoded[sid]:
            decoded['sids'] += 1

    decoded['cia'] = 0
    if rsid:
        decoded['cia'] = 1
    else:
        decoded['cia'] = int(decoded['speed'] == 'CIA')

    decoded['pal'] = int('PAL' in decoded['clock'])

    if decoded['cia']:
        siddir = os.path.dirname(sidfile)
        cmd = ['docker', 'run', '--rm', '-v', f'{siddir}:/tmp', 'sidplayfp',
                '-t1', '-q', '--none', '--cpu-debug', os.path.join('tmp', os.path.basename(sidfile))]
        timer_write_re = re.compile('^.+\s+ST([AXY])a\s+dc0([45])$')
        timer_low = 0
        timer_high = 0

        with subprocess.Popen(cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                shell=False,
                errors='ignore') as process:
            out, _ = process.communicate()
            for line in out.splitlines():
                match = timer_write_re.match(line)
                if match:
                    cpu_reg = match.group(1)
                    cia_reg = int(match.group(2))
                    cpu_reg_map = {'A': 2, 'X': 3, 'Y': 4}
                    raw_val = line.split()[cpu_reg_map[cpu_reg]]
                    val = int(raw_val, 16)
                    if cia_reg == 4:
                        timer_low = val
                    else:
                        timer_high = val
        decoded['cia'] = (timer_high << 8) + timer_low
        # subprocess.check_call(['stty', 'sane'])

    return decoded