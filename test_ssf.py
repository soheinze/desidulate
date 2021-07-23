#!/usr/bin/python3

import os
import tempfile
import unittest
import pandas as pd
from io import StringIO
from sidlib import get_sid, jittermatch_df, reg2state, state2ssfs
from sidmidi import SidMidiFile
from ssf import SidSoundFragment, add_freq_notes_df


class SIDLibTestCase(unittest.TestCase):

    def test_jittermatch(self):
        ssf1 = StringIO('''
clock,frame,freq1,pwduty1,gate1,sync1,ring1,test1,tri1,saw1,pulse1,noise1,atk1,dec1,sus1,rel1,vol,fltlo,fltband,flthi,flt1,fltext,fltres,fltcoff,freq3,test3,freq1nunique,pwduty1nunique,volnunique
0,0,,,1,,,1,,,,,0,0,5,5,15,0,0,1,1,0,15,128,,,1,0,1
19346,1,,,1,,,1,,,,,0,0,5,5,15,0,0,1,1,0,15,640,,,1,0,1
19636,1,50416,,1,0,0,0,0,0,0,1,0,0,5,5,15,0,0,1,1,0,15,640,,,1,0,1
39225,2,50416,,1,0,0,0,0,0,0,1,0,15,5,5,15,0,0,1,1,0,15,640,,,1,0,1
39234,2,50416,,1,0,0,0,0,0,0,1,0,15,0,0,15,0,0,1,1,0,15,640,,,1,0,1
39283,2,50416,,0,0,0,0,0,0,0,1,,,,,15,0,0,1,1,0,15,640,,,1,0,1
''')
        ssf2 = StringIO('''
clock,frame,freq1,pwduty1,gate1,sync1,ring1,test1,tri1,saw1,pulse1,noise1,atk1,dec1,sus1,rel1,vol,fltlo,fltband,flthi,flt1,fltext,fltres,fltcoff,freq3,test3,freq1nunique,pwduty1nunique,volnunique
0,0,,,1,,,1,,,,,0,0,5,5,15,0,0,1,1,0,15,128,,,1,0,1
19410,1,,,1,,,1,,,,,0,0,5,5,15,0,0,1,1,0,15,640,,,1,0,1
19700,1,50416,,1,0,0,0,0,0,0,1,0,0,5,5,15,0,0,1,1,0,15,640,,,1,0,1
39289,2,50416,,1,0,0,0,0,0,0,1,0,15,5,5,15,0,0,1,1,0,15,640,,,1,0,1
39298,2,50416,,1,0,0,0,0,0,0,1,0,15,0,0,15,0,0,1,1,0,15,640,,,1,0,1
39347,2,50416,,0,0,0,0,0,0,0,1,,,,,15,0,0,1,1,0,15,640,,,1,0,1
''')
        df1 = pd.read_csv(ssf1, dtype=pd.UInt64Dtype())
        df2 = pd.read_csv(ssf2, dtype=pd.UInt64Dtype())
        self.assertEqual(
            df1.drop(['clock'], axis=1).to_string(),
            df2.drop(['clock'], axis=1).to_string())
        self.assertTrue(jittermatch_df(df1, df2, 'clock', 1024))
        self.assertFalse(jittermatch_df(df1, df2, 'clock', 32))


class SSFTestCase(unittest.TestCase):
    """Test SSF."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _df2ssf(self, df, percussion=True):
        sid = get_sid(pal=True)
        smf = SidMidiFile(sid, 125)
        df = add_freq_notes_df(sid, df)
        return SidSoundFragment(percussion=percussion, sid=sid, smf=smf, df=df)

    def test_notest_ssf(self):
        df = pd.DataFrame(
            [{'hashid': 1, 'count': 1, 'clock': 0, 'freq1': 1024, 'pwduty1': 0, 'atk1': 0, 'dec1': 0, 'sus1': 15, 'rel1': 0, 'gate1': 1, 'sync1': 0, 'ring1': 0, 'test1': 0, 'tri1': 1, 'saw1': 0, 'pulse1': 0, 'noise1': 0, 'flt1': 0, 'fltres': 0, 'fltcoff': 0, 'fltlo': 0, 'fltband': 0, 'flthi': 0, 'vol': 15},
             {'hashid': 1, 'count': 1, 'clock': 1e5, 'gate1': 0}], dtype=pd.UInt64Dtype())
        s = self._df2ssf(df, percussion=True)
        self.assertEqual(s.waveforms, {'tri'})
        self.assertEqual(s.midi_pitches, (35,))
        self.assertEqual(s.total_duration, 98525)
        self.assertEqual(s.midi_notes, ((0, 35, 98525, 127, 60.134765625),))

    def test_test_ssf(self):
        df = pd.DataFrame(
            [{'hashid': 1, 'count': 1, 'clock': 0, 'freq1': 1024, 'pwduty1': 0, 'atk1': 0, 'dec1': 0, 'sus1': 15, 'rel1': 0, 'gate1': 1, 'sync1': 0, 'ring1': 0, 'test1': 1, 'tri1': 1, 'saw1': 0, 'pulse1': 0, 'noise1': 0, 'flt1': 0, 'fltres': 0, 'fltcoff': 0, 'fltlo': 0, 'fltband': 0, 'flthi': 0, 'vol': 15},
             {'hashid': 1, 'count': 1, 'clock': 2 * 1e4, 'test1': 0},
             {'hashid': 1, 'count': 1, 'clock': 1e5, 'gate1': 0}], dtype=pd.UInt64Dtype())
        s = self._df2ssf(df, percussion=True)
        self.assertEqual(s.waveforms, {'tri'})
        self.assertEqual(s.midi_pitches, (35,))
        self.assertEqual(s.total_duration, 78820)
        self.assertEqual(s.midi_notes, ((20000, 35, 78820, 127, 60.134765625),))

    def test_ssf_parser(self):
        sid = get_sid(pal=True)
        smf = SidMidiFile(sid, 125)
        test_log = os.path.join(self.tmpdir.name, 'vicesnd.log')
        with open(test_log, 'w') as log:
            log.write('\n'.join((
                '1 24 15',
                '1 7 255',
                '1 8 128',
                '1 13 255',
                '100 11 129',
                '100000 11 0')) + '\n')
        ssf_log_df, ssf_df, _, _ = state2ssfs(sid, reg2state(sid, test_log))
        ssf_log_df.reset_index(level=0, inplace=True)
        ssf_df.reset_index(level=0, inplace=True)
        ssf_df = add_freq_notes_df(sid, ssf_df)
        for row in ssf_log_df.itertuples():
            ssf = SidSoundFragment(percussion=True, sid=sid, smf=smf, df=ssf_df[ssf_df['hashid'] == row.hashid])
            if ssf and row.clock == 103:
                self.assertEqual(row.clock, 103)
                self.assertEqual(ssf.midi_pitches, (95,))
                self.assertEqual(ssf.total_duration, 98525)
                self.assertEqual(row.voice, 2)
                return
        self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
