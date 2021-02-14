# Copyright 2020 Josh Bailey (josh@vandervecken.com)

## Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

## The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABL E FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# https://www.c64-wiki.com/wiki/SID

VOICES = {1, 2, 3}

class SidRegEvent:

    def __init__(self, reg, descr, voicenum=None, otherreg=None):
        self.reg = reg
        self.descr = descr
        self.voicenum = voicenum
        self.otherreg = otherreg

    def __str__(self):
        return '%.2x %s voicenum %s otherreg %s' % (self.reg, self.descr, self.voicenum, self.otherreg)

    def __repr__(self):
        return self.__str__()


class SidRegStateBase:

    def __init__(self):
        self.regstate = {}

    def hashreg(self):
        return ''.join(('%2.2x' % int(j) for _, j in sorted(self.regstate.items())))

    def __eq__(self, other):
        return self.hashreg() == other.hashreg()

    def __ne__(self, other):
        return not self.__eq__(other)


class SidRegHandler(SidRegStateBase):

    REGBASE = 0
    NAME = 'unknown'
    REGMAP = {}

    def __init__(self, instance=0):
        self.instance = instance
        self.regstate = {}
        for reg in self.REGMAP:
            self._set(reg, 0)

    def regbase(self):
        return self.REGBASE + (self.instance - 1) * len(self.REGMAP)

    def lohi(self, lo, hi):
        return (self.regstate.get(hi, 0) << 8) + self.regstate.get(lo, 0)

    def lohi_attr(self, lo, hi, attr):
        val = self.lohi(lo, hi)
        setattr(self, attr, val)
        return {attr: '%.2x' % val}

    def byte2nib(self, reg):
        val = self.regstate.get(reg, 0)
        lo, hi = val & 0x0f, val >> 4
        return (lo, hi)

    def byte2nib_literal(self, reg, lo_lit, hi_lit):
        lo, hi = self.byte2nib(reg)
        descrs = {}
        for lit, val in ((lo_lit, lo), (hi_lit, hi)):
            setattr(self, lit, val)
            descrs[lit] = '%.2x' % val
        return descrs

    def decodebits(self, val, decodemap):
        bstates = {}
        for b in decodemap:
            attr = decodemap[b]
            bval = int(bool(val & 2**b))
            setattr(self, attr, bval)
            bstates[attr] = '%u' % bval
        return bstates

    def _set(self, reg, val):
        self.regstate[reg] = val
        descr, otherreg = self.REGMAP[reg](reg)
        preamble = '%s %u %.2x -> %.2x' % (self.NAME, self.instance, val, reg)
        if otherreg is not None:
            otherreg = list(otherreg)[0] + self.regbase()
        return (preamble, descr, otherreg)

    def set(self, reg, val):
        reg -= self.regbase()
        return self._set(reg, val)


class SidVoiceRegState(SidRegHandler):

    REGBASE = 0
    NAME = 'voice'

    def __init__(self, instance):
        self.REGMAP = {
            0: self._freq,
            1: self._freq,
            2: self._pwduty,
            3: self._pwduty,
            4: self._control,
            5: self._attack_decay,
            6: self._sustain_release,
        }
        super(SidVoiceRegState, self).__init__(instance)
        self.voicenum = instance

    def _freq_descr(self):
        return self.lohi_attr(0, 1, 'frequency')

    def _freq(self, reg):
        return (self._freq_descr(), {0, 1} - {reg})

    def _pwduty_descr(self):
        return self.lohi_attr(2, 3, 'pw_duty')

    def _pwduty(self, reg):
        return (self._pwduty_descr(), {2, 3} - {reg})

    def _attack_decay_descr(self):
        return self.byte2nib_literal(5, 'decay', 'attack')

    def _attack_decay(self, _):
        return (self._attack_decay_descr(), None)

    def _sustain_release_descr(self):
        return self.byte2nib_literal(6, 'release', 'sustain')

    def _sustain_release(self, _):
        return (self._sustain_release_descr(), None)

    def _control_descr(self):
        val = self.regstate[4]
        return self.decodebits(val, {
            0: 'gate', 1: 'sync', 2: 'ring', 3: 'test',
            4: 'triangle', 5: 'sawtooth', 6: 'pulse', 7: 'noise'})

    def _control(self, _):
        return (self._control_descr(), None)

    def waveforms(self):
        return {waveform for waveform in ('triangle', 'sawtooth', 'pulse', 'noise') if getattr(self, waveform, None)}

    def any_waveform(self):
        return bool(self.waveforms())

    def gate_on(self):
        # https://codebase64.org/doku.php?id=base:classic_hard-restart_and_about_adsr_in_generally
        if self.test:
            return False
        if not self.gate:
            return False
        return True


class SidFilterMainRegState(SidRegHandler):

    REGBASE = 21
    NAME = 'main'

    def regbase(self):
        return self.REGBASE

    def _filtercutoff(self, reg):
        return (self.lohi_attr(0, 1, 'filter_cutoff'), {0, 1} - {reg})

    def _filterresonanceroute(self, _):
        route, self.filter_res = self.byte2nib(2)
        descr = {'filter_res': '%.2x' % self.filter_res}
        descr.update(self.decodebits(route, {
            0: 'filter_voice1', 1: 'filter_voice2', 2: 'filter_voice3', 3: 'filter_external'}))
        return (descr, None)

    def _filtermain(self, _):
        self.vol, filtcon = self.byte2nib(3)
        descr = {'main_vol': '%.2x' % self.vol}
        descr.update(self.decodebits(filtcon, {
            0: 'filter_low', 1: 'filter_band', 2: 'filter_high', 3: 'mute_voice3'}))
        return(descr, None)

    def __init__(self, instance=0):
        self.REGMAP = {
            0: self._filtercutoff,
            1: self._filtercutoff,
            2: self._filterresonanceroute,
            3: self._filtermain,
        }
        self.vol = 0
        self.filter_res = 0
        super(SidFilterMainRegState, self).__init__(instance)


class FrozenSidRegState(SidRegStateBase):

    def __init__(self, regstate):
        self.voices = {}
        reghandlers = {}
        for voicenum in VOICES:
            voice = SidVoiceRegState(voicenum)
            regbase = voice.regbase()
            for reg in voice.REGMAP:
                reghandlers[regbase + reg] = voice
            self.voices[voicenum] = voice
        mainreghandler = SidFilterMainRegState()
        regbase = mainreghandler.regbase()
        for i in mainreghandler.REGMAP:
            reghandlers[regbase + i] = mainreghandler
        self.regstate = {i: 0 for i in reghandlers}
        for reg, val in regstate.items():
            handler = reghandlers[reg]
            handler.set(reg, val)
            self.regstate[reg] = val

    def reg_voicenum(self, reg):
        handler = self.reghandlers[reg]
        if isinstance(handler, SidVoiceRegState):
            return handler.voicenum
        return None

    def gates_on(self):
        return {voicenum for voicenum in self.voices if self.voices[voicenum].gate_on()}

    def set(self, reg, val):
        raise NotImplementedError


class SidRegState(FrozenSidRegState):

    def __init__(self):
        self.reghandlers = {}
        self.voices = {}
        for voicenum in VOICES:
            voice = SidVoiceRegState(voicenum)
            regbase = voice.regbase()
            for reg in voice.REGMAP:
                self.reghandlers[regbase + reg] = voice
            self.voices[voicenum] = voice
        self.mainreghandler = SidFilterMainRegState()
        regbase = self.mainreghandler.regbase()
        for i in self.mainreghandler.REGMAP:
            self.reghandlers[regbase + i] = self.mainreghandler
        self.regstate = {i: 0 for i in self.reghandlers}
        self.last_descr = {i: {} for i in self.reghandlers}

    def _descr_diff(self, last_descr, descr):
        descr_diff = {k: v for k, v in descr.items() if v != last_descr.get(k, None)}
        descr_txt = ' '.join(('%s: %s' % (k, v) for k, v in sorted(descr_diff.items())))
        return descr_txt

    def set(self, reg, val):
        handler = self.reghandlers[reg]
        preamble, descr, otherreg = handler.set(reg, val)
        if self.regstate[reg] == val:
            return None
        voicenum = None
        if isinstance(handler, SidVoiceRegState):
            voicenum = handler.voicenum
        descr_txt = self._descr_diff(self.last_descr[reg], descr)
        event = SidRegEvent(reg, ' '.join((preamble, descr_txt)), voicenum=voicenum, otherreg=otherreg)
        self.regstate[reg] = val
        self.last_descr[reg] = descr
        return event


frozen_sid_state = {}

def frozen_sid_state_factory(state):
    hashreg = state.hashreg()
    if hashreg not in frozen_sid_state:
        frozen_sid_state[hashreg] = FrozenSidRegState(state.regstate)
    return frozen_sid_state[hashreg]