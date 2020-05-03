#!/usr/bin/python3

# Copyright 2020 Josh Bailey (josh@vandervecken.com)

## Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

## The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# https://codebase64.org/doku.php?id=base:building_a_music_routine
# http://www.ucapps.de/howto_sid_wavetables_1.html

import argparse
from sidlib import clock_to_qn, get_consolidated_changes, get_gate_events, get_reg_changes, get_reg_writes, VOICES
from sidmidi import get_midi_file, get_midi_notes_from_events
from sidwav import get_sid


parser = argparse.ArgumentParser(description='Convert vicesnd.sid log into a MIDI file')
parser.add_argument('--logfile', default='vicesnd.sid', help='log file to read')
parser.add_argument('--midifile', default='reg2midi.mid', help='MIDI file to write')
parser.add_argument('--voicemask', default=','.join((str(v) for v in VOICES)), help='command separated list of SID voices to use')
parser.add_argument('--minclock', default=0, type=int, help='start rendering from this clock value')
parser.add_argument('--maxclock', default=0, type=int, help='if > 0, stop rendering at this clock value')
parser.add_argument('--bpm', default=125, type=int, help='MIDI BPM')
pal_parser = parser.add_mutually_exclusive_group(required=False)
pal_parser.add_argument('--pal', dest='pal', action='store_true', help='Use PAL clock')
pal_parser.add_argument('--ntsc', dest='pal', action='store_false', help='Use NTSC clock')
parser.set_defaults(pal=True)
args = parser.parse_args()
voicemask = set((int(v) for v in args.voicemask.split(',')))

sid = get_sid(pal=args.pal)
reg_writes = get_reg_changes(get_reg_writes(args.logfile), voicemask=voicemask, minclock=args.minclock, maxclock=args.maxclock)
reg_writes_changes = get_consolidated_changes(reg_writes, voicemask)
mainevents, voiceevents = get_gate_events(reg_writes_changes, voicemask)

smf = get_midi_file(args.bpm, max(voicemask))

for voicenum, gated_voice_events in voiceevents.items():
    for event_start, events in gated_voice_events:
        midi_notes = get_midi_notes_from_events(sid, events)
        max_midi_note = max(midi_note[1] for midi_note in midi_notes)
        for clock, pitch, duration, _ in midi_notes:
            qn_clock = clock_to_qn(sid, clock, args.bpm)
            qn_duration = clock_to_qn(sid, duration, args.bpm)
            if qn_duration > 0.1:
                smf.addNote(voicenum-1, voicenum, pitch, qn_clock, qn_duration, 127)

with open(args.midifile, 'wb') as midi_f:
    smf.writeFile(midi_f)