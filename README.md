# desidulate

## Overview

desidulate is a command line python suite of tools for understanding C64/SID music - helping understand how the SID is used, and translating SID music (with limitations, see below) to MIDI format. desidulate works directly on SID register logfiles (as generated by VICE's "-sounddev dump"), so it can parse music from any C64 software (game, demo, .sid file, etc) and can leverage all the fine existing and ongoing work within VICE dealing with hardware quirks, illegal opcodes in play routines, etc.

desidulate is intended to help new SID composers understand the SID more quickly and completely, find unexplored territory, and build upon the SID legacy. desidulate was also written to assist CHIME RED Tesla coil synthesizer research - how to apply SID style synthesis to high voltage audio generators, and to avoid error-prone audio based transcription (such has that built into Ableton Live). desidulate doesn't require extensive dependencies and is intended for command line use (e.g. to analyze 1000s of SID files efficiently).

## Installing

$ ./build.sh


## Transcribing to a MIDI file

desidulate can generate a multitrack SMF (one track for each voice, and additional track for each voice for percussion). The intent is not perfect MIDI reproduction (not possible to missing features in MIDI like standardized support for filter sweeps, etc) but to allow analysis of SID programming techniques (e.g. how a particular kick sound is made), and to allow a composer to have MIDI based devices accompany a C64 composition without complex hardware integration.

A SID register dump is parsed into partitions based on SID voice gate events (e.g. gate off to on). The test bit is accounted for as well, to partition on "hard restart" type events. Then each partition is analyzed for the closest MIDI notes based on frequency, and what waveforms are used while a voice is gated on. More than one note can be generated if the pitch channges sufficiently while the gate is on. Notes then are either classed as "regular" (no use of noise waveform) or "percussion" (use of noise waveform). Percussion events are then partitioned further - events that use the noise waveform only are transcribed to hihat, etc, based on noise length. Snares are detected by use of the noise waveform repeatedly used before and after another waveform. Kick drums are detected by use of a pitch drop.

1. Generate VICE register dump (by default, generates vicesnd.sid in current directory).

$ x64sc -sounddev dump /path/to/sid/or/game.d64

2. Generate MIDI file (by default, generates reg2mid.mid in current directory).

$ ./reg2mid.py vicesnd.sid


## Transcribing to a WAV file

desidulate can generate a WAV file from a ReSID based SID simulation directly from a VICE register dump. This allows the composer to write SID music by directly inputting SID registers (either manually or perhaps from a progressive algorithm) without having to deal with a C64 emulator or an intervening MIDI translation layer (like a SIDStation) that is convenient but also limits expression.

$ ./reg2wav.py vicesnd.sid


## Analyzing a VICE register dump

$ ./reg2log.py vicesnd.sid

desidulate can parse a log file and parse SID state, automatically discarding register writes that do not change the SID state, and provide detailed text output of how the SID is being programmed.


## Ongoing work

* Better transcription of noise waveform only percussion events.
* Pitchbending for following small pitch changes.
* Reduce state requirements for complex SID demos.
* Support for ring/sync modulation.
* Support for SID-based sample playback.
* Export to defMON.

## References

* https://github.com/M3wP/XSID: XSID, SID analyzer and MIDI converter (targets Windows/Visual Studio/Delphi primarily)
* https://haendel.ddns.net/~ken/: JSIDPlay2, C64 emulator with SID register logging (requires Java and is resource intensive on some platforms)
* https://csdb.dk/release/?id=152422: siddump, C64 SID analyzer (some limitations with illegal opcodes and intended for use with .sid files specifically)
