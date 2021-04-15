#!/usr/bin/python3

# Copyright 2020 Josh Bailey (josh@vandervecken.com)

## Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

## The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# https://codebase64.org/doku.php?id=base:building_a_music_routine
# http://www.ucapps.de/howto_sid_wavetables_1.html

import copy
import os
from collections import Counter, defaultdict
from functools import lru_cache
import pandas as pd
from fileio import out_path
from sidmidi import ELECTRIC_SNARE, BASS_DRUM, LOW_TOM, PEDAL_HIHAT, CLOSED_HIHAT, OPEN_HIHAT, ACCOUSTIC_SNARE, CRASH_CYMBAL1


class SidSoundFragment:

    def __init__(self, percussion, sid, smf, df):
        self.undiff_df = self._undiff_df(df)
        self.percussion = percussion
        self.midi_notes = tuple(smf.get_midi_notes_from_events(sid, self._row_state()))
        self.midi_pitches = []
        self.total_duration = 0
        self.max_midi_note = 0
        self.min_midi_note = 0
        self.initial_midi_pitches = []
        if self.midi_notes:
            self.midi_pitches = tuple([midi_note[1] for midi_note in self.midi_notes])
            self.initial_midi_pitches = tuple([midi_note[1] for midi_note in self.midi_notes if midi_note[0] < 1e5])
            self.total_duration = sum(duration for _, _, duration, _, _ in self.midi_notes)
            self.max_midi_note = max(self.midi_pitches)
            self.min_midi_note = min(self.midi_pitches)
        last_clock = 0
        self.waveforms = defaultdict(int)
        self.waveform_order = []
        for row, row_waveforms in self._row_state():
            rel_clock = row.clock - last_clock
            for waveform in row_waveforms:
                self.waveforms[waveform] += rel_clock
            if ((self.waveform_order and self.waveform_order[-1] != row_waveforms) or
                  (not self.waveform_order and row_waveforms)):
                self.waveform_order.append(row_waveforms)
            last_clock = row.clock
        self.waveforms = frozenset(self.waveforms.keys())
        self.noisephases = len([waveforms for waveforms in self.waveform_order if 'noise' in waveforms])
        self.all_noise = self.waveforms == {'noise'}
        self.initial_pitch_drop = False
        if len(self.initial_midi_pitches) > 2:
            first_pitch = self.initial_midi_pitches[0]
            last_pitch = self.initial_midi_pitches[1]
            if first_pitch > last_pitch and first_pitch - last_pitch > 12:
                self.initial_pitch_drop = True
        self.drum_pitches = []
        self.pitches = []
        self._set_pitches(sid)

    def _undiff_df(self, df):
        cs = df.drop('clock', axis=1).fillna(0).cumsum()
        undiff_df = df[['clock']].join(cs)
        assert undiff_df.iloc[0].clock == 0, (undiff_df, df)
        return undiff_df

    def _row_state(self):
        for row in self.undiff_df.itertuples():
            waveforms = frozenset(
                waveform[:-1] for waveform in ('noise1', 'pulse1', 'tri1', 'saw1') if getattr(row, waveform) > 0)
            yield (row, waveforms)

    def drum_noise_duration(self, sid, duration):
        max_duration = sid.clockq
        noise_pitch = None
        for noise_pitch in (PEDAL_HIHAT, CLOSED_HIHAT, OPEN_HIHAT, ACCOUSTIC_SNARE, CRASH_CYMBAL1):
            if duration <= max_duration:
                break
            max_duration *= 2
        return noise_pitch

    def _set_pitches(self, sid):
        if not self.midi_notes:
            return
        clock, _pitch, _duration, velocity, _ = self.midi_notes[0]

        if self.noisephases:
            if self.percussion:
                if self.all_noise:
                    self.drum_pitches.append(
                        (clock, self.total_duration, self.drum_noise_duration(sid, self.total_duration), velocity))
                elif self.noisephases > 1:
                    self.drum_pitches.append(
                        (clock, self.total_duration, ELECTRIC_SNARE, velocity))
                else:
                    if self.initial_pitch_drop:
                        # http://www.ucapps.de/howto_sid_wavetables_1.html
                        self.drum_pitches.append(
                            (clock, self.total_duration, BASS_DRUM, velocity))
                    else:
                        self.drum_pitches.append(
                            (clock, self.total_duration, LOW_TOM, velocity))
        else:
            if self.waveforms == {'pulse'} and self.initial_pitch_drop:
                self.drum_pitches.append(
                    (clock, self.total_duration, BASS_DRUM, velocity))
            else:
                for clock, pitch, duration, velocity, _ in self.midi_notes:
                    assert duration > 0, self.midi_notes
                    self.pitches.append(
                        (clock, duration, pitch, velocity))

    def smf_transcribe(self, smf, first_clock, voicenum):
        for clock, duration, pitch, velocity in self.pitches:
            smf.add_pitch(voicenum, first_clock + clock, duration, pitch, velocity)
        for clock, duration, pitch, velocity in self.drum_pitches:
            smf.add_drum_pitch(voicenum, first_clock + clock, duration, pitch, velocity)


class SidSoundFragmentParser:

    def __init__(self, logfile, percussion, sid):
        self.logfile = logfile
        self.percussion = percussion
        self.sid = sid
        self.single_patches = {}
        self.multi_patches = {}
        self.patch_count = Counter()
        self.ssf_events = []
        self.patch_output = (
            ('single_ssf.txt.xz', self.single_patches),
            ('multi_ssf.txt.xz', self.multi_patches))

    def read_patches(self):
        for ext_patches in self.patch_output:
            ext, patches = ext_patches
            patch_log = out_path(self.logfile, ext)
            if not os.path.exists(patch_log):
                continue
            patches_df = pd.read_csv(patch_log)
            for hashid, df in patches_df.groupby('hashid'):
                self.patch_count[hashid] = df['count'].max()
                patches[hashid] = df.drop(['hashid', 'count'], axis=1)

    def dump_patches(self):
        for ext_patches in self.patch_output:
            ext, patches = ext_patches
            if not patches:
                continue
            out_filename = out_path(self.logfile, ext)
            dfs = []
            for hashid, _ in sorted(self.patch_count.items(), key=lambda x: x[1], reverse=True):
                if hashid in patches:
                    df = patches[hashid]
                    df['hashid'] = hashid
                    df['count'] = self.patch_count[hashid]
                    dfs.append(df)
            df = pd.concat(dfs)
            cols = list(df.columns)
            cols.remove('hashid')
            cols.remove('count')
            df = df[['hashid', 'count'] + cols]
            df.to_csv(out_filename, index=False)

    def dump_events(self):
        out_filename = out_path(self.logfile, 'ssf.txt.xz')
        df = pd.DataFrame(self.ssf_events, columns=('first_clock', 'hashid', 'voicenum'), dtype=pd.Int64Dtype())
        df.to_csv(out_filename, index=False)

    def normalize_voicenum(self, row_voicenum, voicenum):
        if row_voicenum == voicenum:
            return 1
        return 3

    @lru_cache
    def _rename_cols(self, cols, voicenum):
        renamed_cols = []
        for col in cols:
            last_ch = col[-1]
            if last_ch.isdigit():
                renamed_cols.append(col.replace(last_ch, str(self.normalize_voicenum(int(last_ch), voicenum))))
            else:
                renamed_cols.append(col)
        return renamed_cols

    @lru_cache
    def _filter_cols(self, cols):
        return [col for col in cols if col.startswith('flt')]

    def _firsts(self, first_state, voicenums):
        first_row = {'clock': 0}
        fieldnames = ['clock']

        for voicenum in voicenums:
            voicestate = first_state.voices[voicenum]
            for field in voicestate.voice_regs:
                val = getattr(voicestate, field)
                field = '%s%u' % (field, voicenum)
                fieldnames.append(field)
                first_row[field] = val
            flt_v_key = 'flt%u' % voicenum
            fieldnames.append(flt_v_key)
            first_row[flt_v_key] = getattr(first_state.mainreg, flt_v_key)
        for field in first_state.mainreg.filter_common + ['vol', 'mute3']:
            val = getattr(first_state.mainreg, field)
            fieldnames.append(field)
            first_row[field] = val

        return (first_row, fieldnames)

    def _voicediff(self, now, last, voicenum):
        voice_diff = now.diff(last)
        voice_diff = {'%s%u' % (k, voicenum): v for k, v in voice_diff.items()}
        return voice_diff

    def _statediffs(self, first_clock, first_row, first_state, first_frame, voicenums, voicestates):
        last_state = first_state
        orig_diffs = defaultdict(list)
        voice_sounding = {v: first_state.voices[v].sounding() for v in voicenums}
        reg_cumsum = copy.copy(first_row)
        reg_max = copy.copy(first_row)

        def _keep_sum(reg_diff):
            for k, v in reg_diff.items():
                reg_cumsum[k] += v
                reg_max[k] = max(reg_max[k], reg_cumsum[k])
                assert reg_cumsum[k] >= 0

        def _first_sum(reg_diff, reg_clock):
            for k, v in reg_diff.items():
                first_row[k] += v
                reg_max[k] = max(reg_max[k], first_row[k])
            first_clock = reg_clock

        for clock, frame, state, voicestate in voicestates[1:]:
            diff = {}
            filter_diff = {}
            sounding = 0
            for voicenum in voicenums:
                voicestate_now = state.voices[voicenum]
                last_voicestate = last_state.voices[voicenum]
                voice_diff = self._voicediff(voicestate_now, last_voicestate, voicenum)
                filter_diff.update(state.mainreg.diff_filter_vol(voicenum, last_state.mainreg))
                if not voice_sounding[voicenum] and not voicestate_now.sounding():
                    _keep_sum(voice_diff)
                    _first_sum(voice_diff, clock)
                    continue
                sounding += 1
                voice_sounding[voicenum] = True
                diff.update(voice_diff)
            if sounding:
                diff.update(filter_diff)
            else:
                _keep_sum(filter_diff)
                _first_sum(filter_diff, clock)
            if diff:
                frame_clock = (frame - first_frame) * self.sid.clockq
                orig_diffs[frame_clock].append((clock, diff))
                _keep_sum(diff)
            if not voicestate.gate and voicestate.rel == 0:
                break
            last_state = state

        orig_diffs[0] = [(first_clock, first_row)] + orig_diffs[0]
        return (orig_diffs, reg_max)

    def _del_cols(self, voicenums, reg_max):
        del_cols = set()
        filtered_voices = 0
        mute3 = reg_max.get('mute3', 0)
        if not mute3 or 3 not in voicenums:
            del_cols.update('mute3')
        for voicenum in voicenums:
            pulse_col = 'pulse%u' % voicenum
            pw_duty_col = 'pw_duty%u' % voicenum
            if reg_max[pulse_col] == 0:
                del_cols.add(pw_duty_col)
            flt_col = 'flt%u' % voicenum
            if reg_max[flt_col] == 0:
                del_cols.add(flt_col)
            else:
                filtered_voices += 1
        if filtered_voices == 0:
            del_cols.update(self._filter_cols(tuple(reg_max.keys())))
        return del_cols, filtered_voices

    def _compress_diffs(self, orig_diffs, del_cols):
        rows = []
        last_clock = 0
        for frame_clock, clock_diffs in sorted(orig_diffs.items()):
            first_clock, _ = clock_diffs[0]
            for clock, diff in clock_diffs:
                if del_cols:
                    diff = {k: v for k, v in diff.items() if k not in del_cols}
                if diff:
                    diff_clock = clock - first_clock
                    diff['clock'] = frame_clock + diff_clock
                    assert last_clock == 0 or diff['clock'] > last_clock, (orig_diffs, rows)
                    last_clock = diff['clock']
                    rows.append(diff)
        return rows

    def parsedf(self, voicenum, events):
        voicestates = [(clock, frame, state, state.voices[voicenum]) for clock, frame, state in events]
        audible_voicenums = set()
        prev_state = None
        for _, _, state, _ in voicestates:
            audible_voicenums = audible_voicenums.union(*[state.audible_voicenums(prev_state)])
            prev_state = state
        hashid = None
        df = None
        first_clock = None
        voicenums = None

        if voicenum in audible_voicenums:
            first_clock = voicestates[0][0]
            synced_voicenums = frozenset().union(*[voicestate.synced_voicenums() for _, _, _, voicestate in voicestates])
            voicenums = frozenset({voicenum}).union(synced_voicenums)
            assert len(voicenums) in (1, 2)
            first_event = voicestates[0]
            first_clock, first_frame, first_state, first_voicestate = first_event
            assert first_voicestate.gate

            first_row, fieldnames = self._firsts(first_state, voicenums)
            orig_diffs, reg_max = self._statediffs(first_clock, first_row, first_state, first_frame, voicenums, voicestates)
            del_cols, filtered_voices = self._del_cols(voicenums, reg_max)
            rows = self._compress_diffs(orig_diffs, del_cols)
            df = pd.DataFrame(rows, columns=fieldnames, dtype=pd.Int64Dtype())
            df.columns = self._rename_cols(tuple(df.columns), voicenum)
            assert df['clock'].max() > 0 or len(df) == 1, (df, orig_diffs)
            # assert filtered_voices == 0 or df['flt_coff'].max() > 0, (df, orig_diffs)
            hashid = hash(tuple(df.itertuples(index=False, name=None)))
            if len(voicenums) == 1:
                self.single_patches[hashid] = df
            else:
                self.multi_patches[hashid] = df
            self.patch_count[hashid] += 1
            self.ssf_events.append((first_clock, hashid, voicenum))

        return (hashid, df, first_clock, voicenums)