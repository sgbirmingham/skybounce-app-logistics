"""
skybounce_app_logistics
=======================

SkyBounce streaming event engine for logistics / vehicle telemetry.

This is the first application in the SkyBounce app family. It detects
vehicle-behavior events (hard brake, severe impact, trip start, long stop,
etc.) from a live or recorded sensor CSV and emits SkyBounce telemetry
packets either to a file (for validation and offline development) or to
the SkyBounce IPC socket (for real radio transmission).

Layering:
- skybounce_event_rules        rule primitives, thresholds, detectors
- skybounce-ipc-python          protocol codec, IPC client (optional)
- skybounce_app_logistics       this package: orchestrates the above

The streaming engine is event-by-event equivalent to the PC analyzer's
output for the same events (minus the look-backward features that don't
fit streaming: clustering, context windows, packet budget).
"""

__version__ = "0.1.0"
