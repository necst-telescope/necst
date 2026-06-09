#!/usr/bin/env python3
"""Serve a standalone NECST operator-console UI demo.

This tool is intentionally independent of NECST, neclib, ROS, and the real
observation program.  It is a layout and workflow preview for the future
operator console.  All actions are simulated, but every write-like action is
validated both in the browser and on this demo server so the UI can be checked
from the operator's point of view before connecting it to real telescope
commands.
"""

from __future__ import annotations

import argparse
import json
import math
import signal
import sys
import threading
import time
import uuid
import webbrowser
import urllib.parse
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NECST Operator Console Demo</title>
<style>
:root {
  --bg: #0b1020;
  --panel: #11182c;
  --panel2: #151f37;
  --line: #28324a;
  --text: #eaf0ff;
  --muted: #aab6d4;
  --faint: #7380a0;
  --ok: #56d364;
  --warn: #f2cc60;
  --bad: #ff6b6b;
  --accent: #7aa2ff;
  --accent2: #9fd1ff;
  --shadow: rgba(0, 0, 0, 0.35);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  background: radial-gradient(circle at top left, #1b2c58 0, var(--bg) 34rem);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
button, input, select, textarea { font: inherit; }
button {
  border: 1px solid var(--line);
  border-radius: 10px;
  color: var(--text);
  background: #1a2440;
  padding: 8px 12px;
  cursor: pointer;
}
button:hover:not(:disabled) { border-color: var(--accent); background: #202d52; }
button:disabled { opacity: 0.45; cursor: not-allowed; }
button.compact { padding: 7px 10px; min-width: 72px; }
button.primary.large, button.danger.large { min-width: 160px; min-height: 44px; }
input, select, textarea {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 10px;
  color: var(--text);
  background: #0d1428;
  padding: 8px 10px;
  outline: none;
}
input:focus, select:focus, textarea:focus { border-color: var(--accent); }
input[readonly] { color: var(--muted); background: #10182e; }
textarea {
  resize: vertical;
  min-height: 160px;
  max-height: 460px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
label { display: block; color: var(--muted); font-size: 12px; margin: 0 0 4px; }
small { color: var(--faint); }
.header {
  position: sticky;
  top: 0;
  z-index: 20;
  background: rgba(11, 16, 32, 0.94);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--line);
  box-shadow: 0 6px 24px var(--shadow);
}
.header-inner {
  display: grid;
  grid-template-columns: minmax(420px, 1fr) auto;
  gap: 12px;
  align-items: center;
  padding: 10px 16px;
}
.title { display: flex; flex-wrap: wrap; gap: 8px; align-items: baseline; }
.title h1 { margin: 0; font-size: 18px; letter-spacing: 0.01em; }
.title .subtitle { color: var(--muted); font-size: 12px; }
.status-line { display: flex; flex-wrap: wrap; gap: 7px; align-items: center; margin-top: 7px; }
.header-actions { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; align-items: center; }
.badge {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 3px 9px;
  background: rgba(255,255,255,0.04);
  color: var(--muted);
  white-space: nowrap;
}
.badge.ok { border-color: rgba(86,211,100,0.5); color: var(--ok); }
.badge.warn { border-color: rgba(242,204,96,0.55); color: var(--warn); }
.badge.bad { border-color: rgba(255,107,107,0.55); color: var(--bad); }
.badge.info { border-color: rgba(122,162,255,0.55); color: var(--accent2); }
.system-banner {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 12px;
  align-items: center;
  margin: 0 0 12px;
  padding: 12px 14px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(17, 24, 44, 0.94);
  box-shadow: 0 10px 28px var(--shadow);
}
.system-banner .main { min-width: 0; }
.system-banner .label { font-size: 16px; font-weight: 900; letter-spacing: 0.02em; }
.system-banner .detail { margin-top: 2px; color: var(--muted); font-size: 12px; }
.system-banner .safe { color: var(--faint); font-size: 12px; text-align: right; }
.system-banner.ready { border-color: rgba(86,211,100,0.40); }
.system-banner.starting { border-color: rgba(122,162,255,0.65); background: rgba(31, 50, 91, 0.94); }
.system-banner.running { border-color: rgba(242,204,96,0.75); background: rgba(73, 58, 20, 0.94); }
.system-banner.attention { border-color: rgba(255,107,107,0.80); background: rgba(76, 31, 36, 0.96); }
.card.op-starting { border-color: rgba(122,162,255,0.75); box-shadow: 0 0 0 2px rgba(122,162,255,0.13), 0 10px 28px var(--shadow); }
.card.op-running { border-color: rgba(242,204,96,0.85); box-shadow: 0 0 0 2px rgba(242,204,96,0.17), 0 10px 28px var(--shadow); }
.card.op-attention { border-color: rgba(255,107,107,0.85); box-shadow: 0 0 0 2px rgba(255,107,107,0.16), 0 10px 28px var(--shadow); }
.card.op-running > .card-head, .card.op-running > .card-body { background: rgba(242,204,96,0.055); }
.card.op-starting > .card-head, .card.op-starting > .card-body { background: rgba(122,162,255,0.050); }
.card.op-attention > .card-head, .card.op-attention > .card-body { background: rgba(255,107,107,0.060); }
.operation-subcard.op-running { border-color: rgba(242,204,96,0.85); }
.operation-subcard.op-starting { border-color: rgba(122,162,255,0.75); }
.operation-subcard.op-attention { border-color: rgba(255,107,107,0.85); }
button.busy:not(:disabled) { border-color: rgba(242,204,96,0.85); background: #4b3a16; }
button.pending:not(:disabled) { border-color: rgba(122,162,255,0.75); background: #203865; }
.stop-button {
  min-width: 126px;
  min-height: 48px;
  font-weight: 900;
  color: #fff;
  background: #aa2630;
  border-color: #ff8991;
  box-shadow: 0 0 0 2px rgba(255,107,107,0.15);
}
.stop-button:hover:not(:disabled) { background: #c12e39; border-color: #ffb0b5; }
.page { padding: 14px 16px 24px; max-width: 1540px; margin: 0 auto; }
.main-grid {
  display: grid;
  grid-template-columns: minmax(520px, 1.05fr) minmax(430px, 0.95fr);
  gap: 14px;
  align-items: start;
}
.card {
  background: linear-gradient(180deg, rgba(255,255,255,0.035), rgba(255,255,255,0.01)), var(--panel);
  border: 1px solid var(--line);
  border-radius: 16px;
  box-shadow: 0 10px 28px var(--shadow);
  overflow: hidden;
}
.card.sticky { position: sticky; top: 88px; }
.card-head {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
  padding: 13px 14px 8px;
  border-bottom: 1px solid rgba(40,50,74,0.8);
}
.card-head h2 { margin: 0; font-size: 16px; }
.card-head .hint { color: var(--muted); font-size: 12px; }
.card-body { padding: 14px; }
.row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.row3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }
.field { margin-bottom: 11px; }
.actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.primary { background: #1c3f86; border-color: #5e8fff; font-weight: 700; }
.primary:hover:not(:disabled) { background: #2450a5; }
.danger { background: #7a2027; border-color: #d35d68; font-weight: 700; }
.secondary { background: #18213b; }
.ghost { background: transparent; }
button.selected:not(:disabled) { border-color: rgba(86,211,100,0.70); box-shadow: 0 0 0 2px rgba(86,211,100,0.12); }
.run-bar {
  position: sticky;
  top: 78px;
  z-index: 8;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 14px;
  align-items: center;
  margin: -2px -2px 12px;
  padding: 10px;
  border: 1px solid rgba(122,162,255,0.28);
  border-radius: 14px;
  background: rgba(17, 24, 44, 0.96);
  box-shadow: 0 8px 20px rgba(0,0,0,0.25);
}
.run-target { min-width: 0; }
.run-target .name { color: var(--text); font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.run-target .sub { color: var(--muted); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.run-actions { display: flex; gap: 8px; justify-content: flex-end; align-items: center; flex-wrap: nowrap; }
.optional-checks { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin: 2px 0 10px; color: var(--muted); }
.optional-checks .label { color: var(--faint); font-size: 12px; }
.tabs { display: flex; gap: 6px; margin-bottom: 10px; flex-wrap: wrap; }
.tab { padding: 7px 9px; border-radius: 999px; font-size: 13px; }
.tab.active { background: #203865; border-color: var(--accent); color: #fff; }
.panel { display: none; }
.panel.active { display: block; }
.notice {
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 9px 10px;
  margin: 10px 0;
  background: rgba(255,255,255,0.035);
  color: var(--muted);
}
.notice.ok { border-color: rgba(86,211,100,0.45); color: var(--ok); }
.notice.warn { border-color: rgba(242,204,96,0.5); color: var(--warn); }
.notice.bad { border-color: rgba(255,107,107,0.5); color: var(--bad); }
.compact-list { display: grid; gap: 6px; }
.kv { display: grid; grid-template-columns: 142px 1fr; gap: 8px; color: var(--muted); }
.kv b { color: var(--text); font-weight: 600; }
.log {
  height: 260px;
  overflow: auto;
  padding: 10px;
  background: #080d1b;
  border: 1px solid var(--line);
  border-radius: 12px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}
.log-entry { padding: 4px 0; border-bottom: 1px dashed rgba(255,255,255,0.08); }
.log-entry .time { color: var(--faint); }
.log-entry .ok { color: var(--ok); }
.log-entry .bad { color: var(--bad); }
.runtime-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:10px; }
.runtime-card { border:1px solid var(--line); border-radius:12px; padding:10px; background:rgba(255,255,255,0.025); }
.runtime-card h3 { margin:0 0 8px; font-size:13px; color:var(--accent2); }
.runtime-list { display:grid; gap:6px; font-size:12px; color:var(--muted); }
.runtime-list b { color:var(--text); font-weight:600; }
.process-row { border-top:1px dashed rgba(255,255,255,0.10); padding-top:6px; margin-top:6px; color:var(--muted); }
.process-row:first-child { border-top:0; margin-top:0; padding-top:0; }
.mini-actions { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
.process-row button { margin-top:5px; padding:5px 8px; font-size:12px; }
.path-text { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; direction:rtl; text-align:left; }
.copy-path-text { direction:ltr; text-align:left; user-select:text; }
.obs-output-dir {
  display: none;
  margin: 8px 0 12px;
  padding: 10px;
  border: 1px solid rgba(86,211,100,0.45);
  border-radius: 12px;
  background: rgba(86,211,100,0.08);
}
.obs-output-dir.visible { display: block; }
.obs-output-dir .output-title { color: var(--ok); font-weight: 800; margin-bottom: 6px; }
.obs-output-dir .output-path-row { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; align-items: center; }
.obs-output-dir code {
  display: block;
  overflow-x: auto;
  white-space: nowrap;
  padding: 7px 8px;
  border: 1px solid rgba(86,211,100,0.25);
  border-radius: 9px;
  background: rgba(0,0,0,0.22);
  color: var(--text);
}
.obs-output-dir .output-meta { color: var(--muted); font-size: 12px; margin-top: 6px; }
.obs-output-dir .output-meta b { color: var(--text); }
.obs-output-dir .output-meta code { display:inline; padding:0; border:0; background:transparent; color:var(--text); white-space:normal; }
.progress-status-line { margin-top: 6px; color: var(--muted); font-size: 12px; }
.progress-status-line.ok { color: var(--ok); }
.progress-status-line.warn { color: var(--warn); }
.progress-status-line.bad { color: var(--bad); }
.log-browser-text { max-height:220px; overflow:auto; white-space:pre-wrap; word-break:break-word; background:#080d1b; border:1px solid var(--line); border-radius:10px; padding:8px; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:11px; color:var(--muted); }
.log-choice { display:block; width:100%; text-align:left; margin-top:4px; padding:5px 7px; font-size:12px; }
.warning-list { color:var(--warn); }
details {
  margin-top: 10px;
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 8px 10px;
  background: rgba(255,255,255,0.02);
}
summary { cursor: pointer; color: var(--muted); }

.file-chooser-card {
  border: 1px solid rgba(122,162,255,0.24);
  border-radius: 14px;
  padding: 10px;
  margin: 10px 0 12px;
  background: rgba(255,255,255,0.018);
}
.file-chooser-toolbar {
  display: flex;
  gap: 7px;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 8px;
}
.file-chooser-toolbar button { padding: 6px 9px; font-size: 12px; }
.file-chooser-path {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 8px;
  align-items: end;
  margin-bottom: 8px;
}
.server-file-browser {
  height: 210px;
  overflow: auto;
  border: 1px solid var(--line);
  border-radius: 12px;
  background: #080d1b;
  padding: 4px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}
.file-row {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  width: 100%;
  border: 0;
  border-radius: 8px;
  background: transparent;
  color: var(--text);
  text-align: left;
  padding: 6px 8px;
  cursor: pointer;
}
.file-row:hover, .file-row:focus { background: rgba(122,162,255,0.14); outline: none; }
.file-row.directory .file-name { color: var(--accent2); font-weight: 700; }
.file-row.file .file-name { color: var(--text); }
.file-row .file-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.file-row .file-meta { color: var(--faint); font-size: 11px; white-space: nowrap; }
.file-row.selected { background: rgba(86,211,100,0.12); box-shadow: inset 0 0 0 1px rgba(86,211,100,0.28); }
.file-empty { color: var(--muted); padding: 12px; }

.obs-main-row {
  grid-template-columns: minmax(0, 1fr);
  align-items: stretch;
}
.obs-file-field { min-width: 0; }
.obs-file-line {
  display: grid;
  grid-template-columns: minmax(220px, 1fr) auto auto;
  gap: 8px;
  align-items: center;
}
.obs-full-path {
  direction: ltr;
  text-align: left;
  white-space: nowrap;
  overflow-x: auto;
  text-overflow: clip;
}
.path-edit-details { margin-top: 4px; }
.modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: none;
  align-items: center;
  justify-content: center;
  padding: 18px;
  background: rgba(0, 0, 0, 0.62);
  backdrop-filter: blur(6px);
}
.modal-backdrop.open { display: flex; }
.modal-card {
  width: min(980px, calc(100vw - 32px));
  max-height: calc(100vh - 42px);
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  border: 1px solid var(--line);
  border-radius: 16px;
  background: var(--panel);
  box-shadow: 0 24px 60px rgba(0,0,0,0.55);
  overflow: hidden;
}
.modal-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  padding: 12px 14px;
  border-bottom: 1px solid var(--line);
  background: rgba(255,255,255,0.03);
}
.modal-head h3 { margin: 0; font-size: 16px; }
.modal-head .hint { color: var(--muted); font-size: 12px; }
.modal-card .file-chooser-card {
  margin: 0;
  border: 0;
  border-radius: 0;
  display: grid;
  grid-template-rows: auto auto minmax(240px, 1fr) auto;
  min-height: 0;
}
.modal-card .server-file-browser {
  height: auto;
  min-height: 260px;
  max-height: 58vh;
}
.file-chooser-toolbar select {
  flex: 1 1 260px;
  min-width: 180px;
}
.chooser-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  margin-top: 8px;
}
@media (max-width: 760px) {
  .obs-output-dir .output-path-row { grid-template-columns: 1fr; }
  .obs-file-line { grid-template-columns: 1fr; }
  .modal-backdrop { padding: 8px; }
  .modal-card { width: calc(100vw - 16px); max-height: calc(100vh - 16px); }
  .chooser-footer { align-items: stretch; flex-direction: column; }
}


.callout {
  display: grid;
  gap: 4px;
  border-left: 3px solid var(--accent);
  padding: 8px 10px;
  margin-bottom: 10px;
  background: rgba(122,162,255,0.08);
  color: var(--muted);
}
.callout b { color: var(--text); }
.invalid { border-color: var(--bad) !important; }
.valid { border-color: rgba(86,211,100,0.8) !important; }
@media (max-width: 1050px) {
  .header-inner { grid-template-columns: 1fr; }
  .main-grid { grid-template-columns: 1fr; }
  .card.sticky { position: static; }
  .run-bar { position: static; grid-template-columns: 1fr; }
  .run-actions { justify-content: stretch; flex-wrap: wrap; }
  .run-actions button { flex: 1 1 180px; }
  .row, .row3 { grid-template-columns: 1fr; }
  .stop-button { width: 100%; }
}
</style>
</head>
<body>
<header class="header">
  <div class="header-inner">
    <div>
      <div class="title">
        <h1>NECST Operator Console Demo</h1>
        <span class="subtitle">standalone layout preview / no ROS / no telescope command</span>
      </div>
      <div class="status-line" id="statusBadges"></div>
    </div>
    <div class="header-actions">
      <button id="openProgress" class="secondary" title="Start or open the progress-monitor server managed by this console. If this console exits, only a console-owned progress server stops.">Launch progress monitor</button>
      <button id="authorityButton" class="secondary">Acquire authority</button>
      <button id="stopButton" class="stop-button">STOP</button>
    </div>
  </div>
</header>
<main class="page">
  <div class="system-banner ready" id="systemBanner" role="status" aria-live="polite">
    <div class="main">
      <div class="label" id="systemStateLabel">READY</div>
      <div class="detail" id="systemStateDetail">No observation, mount move, or calibration is currently confirmed.</div>
    </div>
    <div class="safe">STOP and ABORT remain available.</div>
  </div>
  <div class="main-grid">
    <section class="card" id="observationCard">
      <div class="card-head">
        <div>
          <h2>Observation setup and start</h2>
          <div class="hint">Choose the obs file and start. Check and Dry run are optional pre-start checks.</div>
        </div>
        <span class="badge info" id="obsStateBadge">Idle</span>
      </div>
      <div class="card-body">
        <div class="run-bar">
          <div class="run-target">
            <div class="name" id="runBarTitle">No obs file selected</div>
            <div class="sub" id="runBarSub">Set the NECST-side directory and filename. Start will run server-side validation before any simulated command.</div>
          </div>
          <div class="run-actions">
            <button id="startObs" class="primary large" disabled title="Start the selected observation sequence. Check and Dry run are optional; Start always performs server-side validation before launching.">Start observation</button>
            <button id="abortObs" class="danger large" title="Abort a running observation sequence and request recorder/gate/progress cleanup. Safety action: kept clickable even if status has not yet detected observing.">ABORT observation</button>
            <button id="clearStaleObs" class="secondary compact" type="button" disabled title="Recover the console from stale progress state after confirming the telescope/recorder are safe. This archives only current progress pointer files; it does not send hardware commands or delete data.">Clear stale state</button>
          </div>
        </div>
        <div id="obsOutputDirBox" class="obs-output-dir" aria-live="polite">
          <div class="output-title">Latest observation data directory</div>
          <div class="output-path-row">
            <code id="obsOutputDirName">not available yet</code>
            <button id="copyObsOutputDir" class="secondary compact" type="button" title="Copy the record directory name for analysis notebooks.">Copy</button>
          </div>
          <div class="output-meta" id="obsOutputDirMeta">Shown when the console knows the latest record name.</div>
        </div>
        <div class="optional-checks" title="Optional checks. They never move the telescope, move the chopper, start recording, open gates, or apply SG settings.">
          <span class="label">Optional before Start:</span>
          <button id="checkObs" class="secondary compact" title="Static obs-file check only: path/name, extension, mode, channel override, and defaults that would be applied. No hardware state changes.">Check</button>
          <button id="dryRunObs" class="secondary compact" disabled title="Build a simulated execution plan. No mount/chopper/recording/gate/SG apply commands are sent.">Dry run</button>
          <small>Start is allowed without these, but it still performs server-side validation.</small>
        </div>
        <div class="callout">
          <b>Start and ABORT are the primary observation actions.</b>
          <span>Check is a safe static check; Dry run is a safe simulated plan. They are optional and never change hardware state.</span>
        </div>
        <div class="row obs-main-row">
          <div class="field">
            <label for="obsMode">Observation mode</label>
            <select id="obsMode" title="Observation modes supported by the obs-file launcher. RSky and SkyDip are calibration actions, not observation modes.">
              <option value="otf">OTF</option>
              <option value="psw">PSW</option>
              <option value="grid">Grid</option>
              <option value="radio_pointing">Radio Pointing</option>
            </select>
          </div>
          <div class="field obs-file-field">
            <label for="obsPath" title="This is the NECST-side path used by Preview, Check, Dry run, and Start.">Obs file on NECST side</label>
            <div class="obs-file-line">
              <input id="obsPath" readonly placeholder="choose an obs file" title="Read-only computed NECST-side path used by Start observation.">
              <button id="openObsChooser" class="primary compact" type="button" title="Open the NECST-side file chooser in a dialog.">Choose...</button>
              <button id="previewCurrentObs" class="secondary compact" type="button" title="Preview the file currently shown in Obs file on NECST side.">Preview</button>
            </div>
            <div id="obsPathFull" class="path-text obs-full-path" title="Full NECST-side path used by Preview, Check, Dry run, and Start.">No obs file selected</div>
            <small>Click Choose to browse files visible to the console process. In Docker, this means Docker/container files.</small>
          </div>
        </div>
        <details class="path-edit-details">
          <summary>Edit path manually</summary>
          <div class="row" style="margin-top:10px">
            <div class="field">
              <label for="recentDir" title="Directory on the NECST-side filesystem. This is editable; selecting a file fills it automatically.">Obs directory on NECST side</label>
              <input id="recentDir" list="recentDirList" placeholder="/path/on/NECST/side" title="Directory used to construct the run path. This is a NECST-side path, not the browser PC path.">
              <datalist id="recentDirList"></datalist>
              <small>NECST-side directory. The file chooser fills this automatically.</small>
            </div>
            <div class="field">
              <label for="obsFilename" title="Filename on the NECST side. Choosing a file fills this automatically.">Obs filename</label>
              <input id="obsFilename" placeholder="e.g. orion_otf.obs" title="Filename to combine with the NECST-side directory. Accepts .obs or .toml.">
              <small>NECST-side filename. The file chooser fills this automatically.</small>
            </div>
          </div>
        </details>
        <div id="obsValidation" class="notice">Choose a NECST-side obs file or enter a NECST-side path.</div>
        <details open>
          <summary>Preview selected obs file</summary>
          <div class="field" style="margin-top:10px">
            <label for="obsPreview">Preview text</label>
            <textarea id="obsPreview" spellcheck="false" placeholder="Select a NECST-side obs file to preview the exact file that Check/Dry run/Start will use." title="NECST-side preview. The Start button uses the same computed run path."></textarea>
            <small id="previewInfo">Preview: empty</small>
          </div>
        </details>
        <details>
          <summary>Optional local preview only</summary>
          <div class="field" style="margin-top:10px">
            <label for="obsFile" title="This reads a file from the browser computer only. It does not provide a Docker/NECST absolute path and is not used for Start.">Choose local file for visual comparison only</label>
            <input id="obsFile" type="file" accept=".obs,.toml,.txt" title="Preview only. The browser does not expose the Docker/NECST absolute path.">
            <small>Use this only to compare text. Observation execution uses the NECST-side path above, not the browser local file.</small>
          </div>
        </details>
        <div id="obsChooserModal" class="modal-backdrop" role="dialog" aria-modal="true" aria-label="Choose NECST-side obs file">
          <div class="modal-card obs-chooser-modal">
            <div class="modal-head">
              <div>
                <h3>Choose NECST-side obs file</h3>
                <div class="hint">This behaves like a file chooser for the filesystem visible to the console process.</div>
              </div>
              <button id="closeObsChooser" class="secondary compact" type="button">Close</button>
            </div>
            <div class="file-chooser-card" aria-label="NECST-side file chooser">
              <div class="file-chooser-toolbar">
                <button id="serverObsHome" class="secondary compact" type="button" title="Return to the selected location.">Home</button>
                <button id="serverObsUp" class="secondary compact" type="button" title="Move to the parent directory.">Up</button>
                <button id="refreshServerObs" class="secondary compact" type="button" title="Refresh the NECST-side directory listing.">Refresh</button>
                <select id="serverObsRoot" title="Quick locations visible to the console process."></select>
              </div>
              <div class="file-chooser-path">
                <input id="serverObsDir" placeholder="type or browse a folder visible to the console" title="Directory to list on the NECST/console side. This is not the browser PC path.">
                <button id="goServerObsDir" class="secondary compact" type="button" title="Open the typed NECST-side folder.">Open</button>
              </div>
              <div id="serverObsFiles" class="server-file-browser" role="listbox" aria-label="NECST-side files">
                <div class="file-empty">Loading...</div>
              </div>
              <div class="chooser-footer">
                <small id="serverObsInfo">NECST-side file chooser: not loaded</small>
                <button id="useSelectedObs" class="primary compact" type="button" disabled>Use selected file</button>
              </div>
            </div>
          </div>
        </div>
        <details>
          <summary>Advanced observation options</summary>
          <div class="field" style="margin-top:10px">
            <label for="obsChannel">Channel override</label>
            <input id="obsChannel" inputmode="numeric" placeholder="empty = obs file/default">
          </div>
        </details>
      </div>
    </section>

    <section class="card sticky" id="manualCard">
      <div class="card-head">
        <div>
          <h2>Manual telescope control</h2>
          <div class="hint">Right-side control area aligned with the always-visible STOP button.</div>
        </div>
        <span class="badge" id="manualBadge">Ready</span>
      </div>
      <div class="card-body">
        <div class="compact-list" style="margin-bottom:12px">
          <div class="kv"><span>Motion state</span><b id="motionSummary">idle</b></div>
          <div class="kv"><span>Active task</span><b id="taskSummary">none</b></div>
          <div class="kv"><span>Current Az / El</span><b id="posSummary">180.000 / 45.000</b></div>
          <div class="kv"><span>Command Az / El</span><b id="cmdSummary">- / -</b></div>
          <div class="kv"><span>Chopper</span><b id="chopperSummary">OUT / pos 19700</b></div>
        </div>
        <div class="tabs">
          <button class="tab active" data-panel="mountPanel">Mount Az/El</button>
          <button class="tab" data-panel="targetPanel">Target tracking</button>
          <button class="tab" data-panel="chopperPanel">Chopper</button>
          <button class="tab" data-panel="calPanel">RSky / SkyDip</button>
        </div>

        <div id="mountPanel" class="panel active">
          <div class="callout">
            <b>Mount Az/El means mechanical mount angle.</b>
            <span>Az=360 is not automatically treated as Az=0. Invalid inputs are not sent, and the reason is shown.</span>
          </div>
          <div class="row">
            <div class="field">
              <label for="mountAz" title="Mechanical mount Azimuth in degrees. Valid range comes from the active site TOML, not from the GUI.">Command mount Az [deg]</label>
              <input id="mountAz" inputmode="decimal" placeholder="e.g. 180.0" title="Decimal degrees. Mechanical mount Az, not sky Az. Must be within the site TOML Az limit.">
            </div>
            <div class="field">
              <label for="mountEl" title="Mechanical mount Elevation in degrees. Valid range comes from the active site TOML, not from the GUI.">Command mount El [deg]</label>
              <input id="mountEl" inputmode="decimal" placeholder="e.g. 45.0" title="Decimal degrees. Mechanical mount El. Must be within the site TOML El limit.">
            </div>
          </div>
          <div class="notice" id="limitSummary">Site TOML limit: loading...</div>
          <small>Limits are supplied by the console server and are rechecked server-side before any command is sent.</small>
          <div id="mountValidation" class="notice">Enter Az/El.</div>
          <div class="actions">
            <button id="moveMount" class="primary" disabled title="Send a mechanical mount Az/El move only after client and server validation pass.">Move mount</button>
            <button id="dryRunMount" class="secondary" disabled title="Validate and log the mount command without changing demo state.">Dry-run</button>
          </div>
        </div>

        <div id="targetPanel" class="panel">
          <div class="row">
            <div class="field">
              <label for="targetKind">Target kind</label>
              <select id="targetKind" title="Choose a built-in solar-system target, object name, RA/Dec, or Galactic coordinates.">
                <option value="sun">Sun</option>
                <option value="moon">Moon</option>
                <option value="name">Object name</option>
                <option value="radec">RA/Dec</option>
                <option value="galactic">Galactic l/b</option>
              </select>
            </div>
            <div class="field">
              <label for="cosCorrection">cos correction</label>
              <select id="cosCorrection" title="Default for offset coordinate handling. The real implementation should use site/user defaults but allow explicit override here.">
                <option value="true" selected>on</option>
                <option value="false">off</option>
              </select>
            </div>
          </div>
          <div id="targetNameBlock" class="field" style="display:none">
            <label for="targetName">Object name</label>
            <input id="targetName" placeholder="e.g. Orion-KL" title="Object name resolved by the real telescope system. The demo only validates that it is not empty.">
          </div>
          <div id="coordBlock" class="row" style="display:none">
            <div class="field">
              <label id="coord1Label" for="coord1">RA or l [deg]</label>
              <input id="coord1" inputmode="text" title="For RA/Dec: RA accepts decimal degrees, HH:MM:SS.s, or 05h35m17.3s. For Galactic: decimal degrees.">
            </div>
            <div class="field">
              <label id="coord2Label" for="coord2">Dec or b [deg]</label>
              <input id="coord2" inputmode="text" title="For RA/Dec: Dec accepts decimal degrees, ±DD:MM:SS.s, or -05d23m28s. For Galactic: decimal degrees.">
            </div>
          </div>
          <div class="row3">
            <div class="field">
              <label for="offsetFrame">Offset frame</label>
              <select id="offsetFrame" title="Coordinate frame in which offset X/Y are interpreted.">
                <option value="target_frame">target frame</option>
                <option value="altaz">AltAz</option>
                <option value="radec">RA/Dec</option>
                <option value="galactic">Galactic</option>
              </select>
            </div>
            <div class="field">
              <label for="offsetX">Offset X [arcsec]</label>
              <input id="offsetX" value="0" inputmode="decimal" title="Offset X in arcseconds. The real implementation converts this to degrees internally.">
            </div>
            <div class="field">
              <label for="offsetY">Offset Y [arcsec]</label>
              <input id="offsetY" value="0" inputmode="decimal" title="Offset Y in arcseconds. The real implementation converts this to degrees internally.">
            </div>
          </div>
          <div id="targetValidation" class="notice">Target tracking can be started.</div>
          <div class="actions">
            <button id="startTracking" class="primary">Start tracking</button>
            <button id="stopTracking" class="secondary" title="Stops target tracking by sending the same antenna-stop operation used by STOP, but is enabled only for tracking context.">Stop tracking</button>
          </div>
        </div>

        <div id="chopperPanel" class="panel">
          <div class="compact-list">
            <div class="kv"><span>Chopper state</span><b id="chopperState">OUT</b></div>
            <div class="kv"><span>Position</span><b id="chopperPos">19700</b></div>
            <div class="kv"><span>Status age</span><b id="chopperAge">live demo</b></div>
          </div>
          <div class="notice ok">The current chopper state is shown continuously. The status button only logs a manual refresh.</div>
          <div class="actions">
            <button id="chopperIn" class="secondary" aria-pressed="false">Chopper IN</button>
            <button id="chopperOut" class="primary selected" aria-pressed="true">Chopper OUT (current)</button>
            <button id="chopperStatus" class="ghost">Log status</button>
          </div>
          <details>
            <summary>Maintenance actions</summary>
            <div class="notice warn">Home/recover/alarm reset are maintenance actions and should not be visually dominant.</div>
            <div class="actions">
              <button id="chopperAlarmReset" class="secondary">Alarm reset</button>
              <button id="chopperHome" class="secondary">Home</button>
              <button id="chopperRecover" class="danger">Recover</button>
            </div>
          </details>
        </div>

        <div id="calPanel" class="panel">
          <div class="notice" id="calStatusNotice">RSky / SkyDip running state is also shown in the header Active badge.</div>
          <div class="row">
            <div class="card operation-subcard" id="rskyCard" style="box-shadow:none">
              <div class="card-head"><h2>RSky</h2><span class="hint">runtime parameters</span></div>
              <div class="card-body">
                <div class="row">
                  <div class="field"><label for="rskyN">n</label><input id="rskyN" type="number" min="1" step="1" value="1" inputmode="numeric"></div>
                  <div class="field"><label for="rskyInteg">integ [s]</label><input id="rskyInteg" type="number" min="0.1" step="0.5" value="5" inputmode="decimal"></div>
                </div>
                <details><summary>Advanced</summary><div class="field" style="margin-top:10px"><label for="rskyCh">channel</label><input id="rskyCh" placeholder="empty = default"></div></details>
                <button id="runRsky" class="primary">Run RSky</button>
              </div>
            </div>
            <div class="card operation-subcard" id="skydipCard" style="box-shadow:none">
              <div class="card-head"><h2>SkyDip</h2><span class="hint">runtime parameters</span></div>
              <div class="card-body">
                <div class="field"><label for="skydipInteg">integ [s]</label><input id="skydipInteg" type="number" min="0.1" step="0.5" value="10" inputmode="decimal"></div>
                <details><summary>Advanced</summary>
                  <div class="field" style="margin-top:10px"><label for="skydipCh">channel</label><input id="skydipCh" placeholder="empty = default"></div>
                  <div class="field"><label for="skydipTpRange">tp_range</label><input id="skydipTpRange" placeholder="e.g. 5000 6000 17000 18000"></div>
                </details>
                <button id="runSkydip" class="primary">Run SkyDip</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  </div>

  <section class="card" style="margin-top:14px">
    <div class="card-head">
      <div>
        <h2>Operation log</h2>
        <div class="hint">Actions accepted by the console server and validation results.</div>
      </div>
      <button id="clearLog" class="secondary">Clear</button>
    </div>
    <div class="card-body">
      <div id="log" class="log"></div>
    </div>
  </section>

  <section class="card" style="margin-top:14px">
    <div class="card-head">
      <div>
        <h2>Runtime status</h2>
        <div class="hint">Site config, process lifecycle, progress monitor ownership, and persistent log paths.</div>
      </div>
      <button id="refreshRuntime" class="secondary">Refresh</button>
    </div>
    <div class="card-body">
      <div class="runtime-grid">
        <div class="runtime-card">
          <h3>Site and action mode</h3>
          <div class="runtime-list">
            <div>Action mode: <b id="runtimeActionMode">loading</b></div>
            <div>Live writes: <b id="runtimeLiveWrites">loading</b></div>
            <div>Status refresh: <b id="runtimeStatusRefresh">loading</b></div>
            <div>Live telemetry: <b id="runtimeLiveTelemetry">loading</b></div>
            <div>Observatory: <b id="runtimeObservatory">loading</b></div>
            <div>Config source: <b id="runtimeConfigSource">loading</b></div>
            <div class="path-text" title="site config path">Path: <b id="runtimeConfigPath">loading</b></div>
            <div>Capabilities: <b id="runtimeCapabilities">loading</b></div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Process lifecycle</h3>
          <div class="runtime-list">
            <div>Counts: <b id="runtimeProcessCounts">loading</b></div>
            <div id="runtimeProcesses">loading</div>
            <div class="mini-actions">
              <button id="terminateObsLauncher" class="secondary" title="Terminate local observation launcher subprocess only; does not send telescope STOP">Terminate obs launcher</button>
              <button id="terminateCalLauncher" class="secondary" title="Terminate local calibration launcher subprocess only; does not send telescope STOP">Terminate cal launcher</button>
              <button id="terminateAllLaunchers" class="secondary" title="Terminate all local launcher subprocesses started by this console">Terminate all launchers</button>
            </div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Current observation data</h3>
          <div class="runtime-list">
            <div>Record: <b id="runtimeRecordName">loading</b></div>
            <div class="path-text" title="Local PC path if the Docker/container data root is mapped; otherwise the container path is shown.">Local/data path: <b id="runtimeLocalRecordingDir">loading</b></div>
            <div class="path-text" title="Expected NECST recorder output directory inside the console/container environment">Container data directory: <b id="runtimeRecordingDir">loading</b></div>
            <div class="path-text" title="Progress sidecar directory">Progress directory: <b id="runtimeProgressRecordDir">loading</b></div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Progress monitor and logs</h3>
          <div class="runtime-list">
            <div>Progress: <b id="runtimeProgressMonitor">loading</b></div>
            <div class="path-text" title="operator log path">Operator log: <b id="runtimeOperatorLog">loading</b></div>
            <div class="path-text" title="launcher log directory">Launcher logs: <b id="runtimeLauncherLogDir">loading</b></div>
            <div class="mini-actions">
              <button id="loadOperatorLog" class="secondary" title="Read the persistent operator_console.jsonl tail">Read operator log</button>
            </div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Log browser</h3>
          <div class="runtime-list">
            <div id="runtimeLogBrowserSummary">Operator and launcher logs are read-only.</div>
            <div id="runtimeLauncherLogChoices"><span style="color:var(--faint)">No launcher log recorded.</span></div>
            <div id="runtimeLogBrowserText" class="log-browser-text">Select a log to display its tail.</div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Shutdown cleanup</h3>
          <div class="runtime-list">
            <div>Status: <b id="runtimeShutdownStatus">loading</b></div>
            <div>Terminate launchers: <b id="runtimeShutdownTerminate">loading</b></div>
            <div>Last cleanup: <b id="runtimeShutdownLast">loading</b></div>
            <div id="runtimeShutdownSummary" class="path-text">loading</div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Self-check</h3>
          <div class="runtime-list">
            <div>Status: <b id="runtimeSelfCheckStatus">not run</b></div>
            <div id="runtimeSelfCheckSummary" class="path-text">Run self-check before live operation.</div>
            <div class="mini-actions">
              <button id="runSelfCheck" class="secondary" title="Run read-only console self-checks; no telescope command is sent">Run self-check</button>
            </div>
          </div>
        </div>
        <div class="runtime-card">
          <h3>Warnings</h3>
          <div id="runtimeWarnings" class="runtime-list warning-list">loading</div>
        </div>
      </div>
    </div>
  </section>
</main>
<script>
const qs = (id) => document.getElementById(id);
const state = {
  sessionId: localStorage.getItem('necstConsoleDemoSession') || crypto.randomUUID(),
  obsChecked: false,
  siteLimits: {az_min: 5, az_max: 355, el_min: 5, el_max: 85},
  capabilities: {},
  liveActions: {guarded: false, enabled: false},
  lastSelfCheck: null,
  lastStatus: null,
  pendingOperation: null,
  currentOperation: {phase: 'ready', kind: 'idle', label: 'READY'},
  lastActionResponse: null,
  demoObsBrowser: false,
  selectedObsPath: "",
  progressLaunch: {active: false, openedUrl: "", message: ""}
};
localStorage.setItem('necstConsoleDemoSession', state.sessionId);

const FORM_STORAGE_PREFIX = 'necstConsole.form.';
const PERSISTED_FORM_FIELDS = [
  'obsMode', 'recentDir', 'obsFilename', 'obsChannel',
  'mountAz', 'mountEl',
  'targetKind', 'targetName', 'coord1', 'coord2', 'offsetFrame', 'offsetX', 'offsetY', 'cosCorrection',
  'rskyN', 'rskyInteg', 'rskyCh',
  'skydipInteg', 'skydipCh', 'skydipTpRange'
];
function storageKeyForField(id) { return FORM_STORAGE_PREFIX + id; }
function saveFormField(id) {
  const el = qs(id);
  if (!el) return;
  try { localStorage.setItem(storageKeyForField(id), el.value); } catch (_) {}
}
function restoreFormField(id) {
  const el = qs(id);
  if (!el) return;
  try {
    const saved = localStorage.getItem(storageKeyForField(id));
    if (saved !== null && saved !== undefined) el.value = saved;
  } catch (_) {}
}
function setupFormPersistence() {
  PERSISTED_FORM_FIELDS.forEach(id => {
    restoreFormField(id);
    const el = qs(id);
    if (!el) return;
    ['input', 'change'].forEach(ev => el.addEventListener(ev, () => saveFormField(id)));
  });
}

function numberOrNaN(value) {
  if (String(value).trim() === '') return NaN;
  return Number(value);
}
function finite(value) { return Number.isFinite(value); }
function finiteStatusNumber(value) {
  return value !== null && value !== undefined && String(value).trim() !== '' && Number.isFinite(Number(value));
}

function parseSexagesimalAngle(value, isRa) {
  const raw = String(value || '').trim();
  if (!raw) return {ok: false, reason: 'empty coordinate'};
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && /^[-+]?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(raw)) {
    return {ok: true, deg: numeric, format: 'decimal degrees'};
  }
  let s = raw.replace(/−/g, '-').trim().toLowerCase();
  let sign = 1;
  if (s.startsWith('+')) s = s.slice(1).trim();
  if (s.startsWith('-')) { sign = -1; s = s.slice(1).trim(); }
  if (isRa && sign < 0) return {ok: false, reason: 'RA cannot be negative'};
  s = s
    .replace(/[hHdD°]/g, ':')
    .replace(/[mM′']/g, ':')
    .replace(/[sS″"]/g, '')
    .replace(/\s+/g, ':')
    .replace(/:+/g, ':')
    .replace(/^:/, '')
    .replace(/:$/, '');
  const parts = s.split(':').filter(x => x !== '').map(Number);
  if (parts.length < 1 || parts.length > 3 || !parts.every(Number.isFinite)) {
    return {ok: false, reason: isRa ? 'RA must be decimal degrees or HH:MM:SS.s' : 'Dec must be decimal degrees or ±DD:MM:SS.s'};
  }
  const a0 = Math.abs(parts[0]);
  const a1 = parts.length > 1 ? parts[1] : 0;
  const a2 = parts.length > 2 ? parts[2] : 0;
  if (a1 < 0 || a1 >= 60 || a2 < 0 || a2 >= 60) {
    return {ok: false, reason: 'sexagesimal minutes/seconds must be in 0..60'};
  }
  const sex = a0 + a1 / 60 + a2 / 3600;
  const deg = isRa ? sex * 15 : sign * sex;
  return {ok: true, deg, format: isRa ? 'sexagesimal hours' : 'sexagesimal degrees'};
}
function setNotice(el, kind, text) {
  el.className = 'notice' + (kind ? ' ' + kind : '');
  el.textContent = text;
}
function markInput(el, ok) {
  el.classList.remove('valid', 'invalid');
  if (ok === true) el.classList.add('valid');
  if (ok === false) el.classList.add('invalid');
}

function setText(id, text) {
  const el = qs(id);
  if (el) el.textContent = text;
}
function setDisabled(id, disabled) {
  const el = qs(id);
  if (el) el.disabled = Boolean(disabled);
}
function formatMaybeNumber(value, digits=4) {
  return finiteStatusNumber(value) ? Number(value).toFixed(digits) : '-';
}
const PENDING_ATTENTION_SEC = 15;
function lowerText(value) { return String(value ?? '').trim().toLowerCase(); }
function processCategoryActive(data, category) {
  const records = Array.isArray(data && data.processes) ? data.processes : [];
  const want = lowerText(category);
  return records.some(r => {
    const active = Boolean(r && (r.active || r.is_active || r.status === 'running' || r.status === 'active'));
    if (!active) return false;
    const haystack = [r.category, r.kind, r.name, r.action, r.label, r.command].map(lowerText).join(' ');
    return haystack === want || haystack.includes(want);
  });
}
function processMatchesKind(record, kind) {
  if (!record || !kind) return false;
  const k = lowerText(kind);
  const haystack = [record.category, record.kind, record.name, record.action, record.label, record.command].map(lowerText).join(' ');
  if (k === 'observation') return haystack.includes('observation') || haystack.includes('start_observation');
  if (k === 'rsky') return haystack.includes('rsky');
  if (k === 'skydip') return haystack.includes('skydip');
  if (k === 'calibration') return haystack.includes('calibration') || haystack.includes('rsky') || haystack.includes('skydip');
  return haystack.includes(k);
}
function matchingRecentFinalProcess(data, pending) {
  if (!pending || !['observation', 'rsky', 'skydip', 'calibration'].includes(pending.kind)) return null;
  const records = Array.isArray(data && data.processes) ? data.processes : [];
  const startedAt = Number(pending.startedAt || 0) / 1000.0;
  const candidates = records.filter(r => {
    if (!processMatchesKind(r, pending.kind)) return false;
    const status = lowerText(r.status);
    if (!['exited', 'failed', 'lost', 'unknown'].includes(status)) return false;
    const recStart = Number(r.started_at || 0);
    if (Number.isFinite(startedAt) && startedAt > 0 && Number.isFinite(recStart) && recStart + 2.0 < startedAt) return false;
    return true;
  });
  if (!candidates.length) return null;
  candidates.sort((a, b) => Number(b.started_at || 0) - Number(a.started_at || 0));
  return candidates[0];
}
const MOUNT_REACHED_TOL_DEG = 5.0e-4;
function mountPendingReached(data, pending) {
  if (!pending || pending.kind !== 'mount') return false;
  const targetAz = Number(pending.targetAz);
  const targetEl = Number(pending.targetEl);
  const currentAz = Number(data && data.az);
  const currentEl = Number(data && data.el);
  const azDelta = Math.min(Math.abs(currentAz - targetAz), Math.abs(((currentAz - targetAz + 180.0) % 360.0) - 180.0));
  const elDelta = Math.abs(currentEl - targetEl);
  return Number.isFinite(targetAz) && Number.isFinite(targetEl)
    && Number.isFinite(currentAz) && Number.isFinite(currentEl)
    && azDelta <= MOUNT_REACHED_TOL_DEG
    && elDelta <= MOUNT_REACHED_TOL_DEG;
}
function exclusiveStartActionKind(action) {
  const a = lowerText(action || '');
  if (a === 'start_observation') return 'observation';
  if (a === 'mount_move') return 'mount';
  if (a === 'run_rsky') return 'rsky';
  if (a === 'run_skydip') return 'skydip';
  if (a === 'start_tracking') return 'tracking';
  return '';
}
function operationFromExclusiveStartGuard(data) {
  const guard = data && data.exclusive_start_guard;
  if (!guard || typeof guard !== 'object') return null;
  const kind = exclusiveStartActionKind(guard.action);
  if (!kind) return null;
  const age = Number(guard.age_sec);
  const limit = Number(guard.guard_sec ?? PENDING_ATTENTION_SEC);
  if (!Number.isFinite(age) || !Number.isFinite(limit) || age < 0 || age > limit) return null;
  const text = pendingLabel({kind});
  const message = String(guard.message || guard.action || 'start command');

  // If this page was reloaded after the command and the launcher already
  // finished, do not keep showing STARTING.  A clean quick exit returns to
  // READY; a failed exit becomes ATTENTION so the operator is guided to logs.
  const started = Number(guard.started_at || 0);
  const finalProcess = matchingRecentFinalProcess(data || {}, {kind, startedAt: started > 0 ? started * 1000.0 : 0});
  if (finalProcess) {
    const rc = finalProcess.returncode;
    const okFinal = rc === 0 || rc === '0';
    if (okFinal) {
      return {
        phase: 'ready',
        kind: 'idle',
        label: 'READY',
        detail: `${message} finished before this status view confirmed RUNNING; launcher exited normally.`
      };
    }
    return {
      phase: 'attention',
      kind,
      label: 'ATTENTION',
      detail: `${message} launcher ended with status=${finalProcess.status || 'unknown'}, returncode=${rc ?? 'unknown'}. Check launcher logs before starting again.`
    };
  }

  return {
    phase: 'starting',
    kind,
    label: text.label,
    detail: `${message} was accepted ${age.toFixed(1)} s ago; waiting for status confirmation. This may have been started from another browser or before this page was reloaded.`
  };
}
function statusHasCommand(data) {
  return finiteStatusNumber((data || {}).command_az) && finiteStatusNumber((data || {}).command_el);
}
function safetyReleaseLooksIdle(data, ageSec) {
  data = data || {};
  const sys = lowerText(data.state || 'idle');
  const finalOrIdle = ['idle', 'finished', 'aborted', 'error', 'failed', 'unknown'].includes(sys);
  if (!finalOrIdle || statusHasCommand(data)) return false;
  if (data.operator_safety_release_idle) return true;
  const src = lowerText(data.motion_live_source || '');
  // Backend versions before this patch may keep stale SKY/moving or
  // encoder-delta hysteresis after STOP.  STOP/ABORT are release requests, so
  // once the command is gone and the lifecycle is final/idle, do not let the UI
  // remain in STOP REQUESTED until ATTENTION.  Use a short grace to avoid
  // flipping READY while a command topic is still disappearing.
  if (ageSec >= 1.5 && src !== 'encoder_delta') return true;
  if (ageSec >= 3.0) return true;
  return false;
}
function actualOperationFromStatus(data) {
  data = data || {};
  const stale = data.stale_observation || {};
  if (stale.running_stale) {
    const age = Number(stale.age_sec);
    const ageText = Number.isFinite(age) ? `${age.toFixed(1)} s` : 'unknown age';
    const record = stale.record_name || 'unknown record';
    return {
      phase: 'attention',
      kind: 'observation',
      label: 'ATTENTION: STALE OBSERVATION STATE',
      detail: `Progress still says observation is running for ${record}, but no active launcher/fresh progress update is confirmed (${ageText}). Confirm the hardware is safe, then use Clear stale state.`
    };
  }
  const sys = lowerText(data.state || 'idle');
  const manual = lowerText(data.manual_state || 'idle');
  const task = lowerText(data.active_task || 'none');
  const progress = data.progress || {};
  const finalOrIdle = ['idle', 'finished', 'aborted', 'error', 'failed'].includes(sys);
  const obsActive = sys === 'observing' || (!finalOrIdle && task.includes('observation')) || (!finalOrIdle && Boolean(progress.observation_running)) || processCategoryActive(data, 'observation');
  const calActive = sys === 'calibrating' || (!finalOrIdle && manual === 'calibration') || (!finalOrIdle && (task.includes('rsky') || task.includes('skydip'))) || processCategoryActive(data, 'calibration');
  const explicitTrackingActive = task.includes('target tracking');
  const atTargetIdle = Boolean((data.mount_target_reached || data.mount_hold_at_target) && data.motion_live_active === false);
  const mountActive = !atTargetIdle && (manual === 'moving' || (manual === 'tracking' && !explicitTrackingActive) || task.includes('mount move') || task.includes('manual mount'));
  const trackingActive = !atTargetIdle && (explicitTrackingActive || manual === 'target tracking');
  if (obsActive) {
    return {phase: 'running', kind: 'observation', label: 'OBSERVATION RUNNING', detail: 'Observation execution is confirmed by status/progress/launcher state.'};
  }
  if (calActive) {
    const kind = task.includes('skydip') ? 'skydip' : (task.includes('rsky') ? 'rsky' : 'calibration');
    const label = kind === 'skydip' ? 'SKYDIP RUNNING' : (kind === 'rsky' ? 'RSKY RUNNING' : 'CALIBRATION RUNNING');
    return {phase: 'running', kind, label, detail: 'Calibration execution is confirmed by status or launcher state.'};
  }
  if (mountActive) {
    return {phase: 'running', kind: 'mount', label: 'MOUNT MOVING', detail: 'Mount motion is confirmed by live/manual status.'};
  }
  if (trackingActive) {
    return {phase: 'running', kind: 'tracking', label: 'TARGET TRACKING', detail: 'Time-dependent target tracking is active. Fixed Az/El mount-move hold is not treated as target tracking.'};
  }
  const guardedStart = operationFromExclusiveStartGuard(data);
  if (guardedStart) return guardedStart;
  return {phase: 'ready', kind: 'idle', label: 'READY', detail: 'No observation, mount move, or calibration is currently confirmed.'};
}
function pendingLabel(pending) {
  if (!pending) return {label: 'STARTING...', detail: 'Command was accepted by the console; waiting for status confirmation.'};
  const map = {
    observation: ['STARTING OBSERVATION...', 'Start was accepted; waiting until observation status/progress confirms running.'],
    mount: ['MOUNT COMMAND SENT', 'Mount command was accepted; waiting for motion confirmation.'],
    rsky: ['STARTING RSKY...', 'RSky command was accepted; waiting for calibration status/launcher confirmation.'],
    skydip: ['STARTING SKYDIP...', 'SkyDip command was accepted; waiting for calibration status/launcher confirmation.'],
    tracking: ['STARTING TRACKING...', 'Tracking command was accepted; waiting for tracking status confirmation.'],
    stop: ['STOP REQUESTED...', 'STOP was sent; waiting for idle status.'],
    abort: ['ABORT REQUESTED...', 'ABORT was sent; waiting for idle status.']
  };
  const pair = map[pending.kind] || ['STARTING...', 'Command was accepted; waiting for status confirmation.'];
  return {label: pair[0], detail: pair[1]};
}
function deriveOperationState(data) {
  const actual = actualOperationFromStatus(data || {});
  const pending = state.pendingOperation;
  if (pending) {
    const ageSec = (Date.now() - Number(pending.startedAt || Date.now())) / 1000;
    const text = pendingLabel(pending);
    const isSafetyPending = pending.kind === 'stop' || pending.kind === 'abort';

    // STOP/ABORT are user intent, not normal START operations.  If the real
    // status is still RUNNING immediately after a safety button was pressed,
    // show the operator that the stop/abort request is being waited on rather
    // than snapping the banner back to RUNNING.  External CLI stop/abort still
    // clears this as soon as /api/status reports READY.
    if (isSafetyPending) {
      if (actual.phase === 'ready' || safetyReleaseLooksIdle(data || {}, ageSec)) {
        state.pendingOperation = null;
        return {
          phase: 'ready',
          kind: 'idle',
          label: 'READY',
          detail: 'STOP/ABORT release is complete for the operator console: command output is gone and the system lifecycle is final/idle.'
        };
      }
      if (ageSec > PENDING_ATTENTION_SEC) {
        return {
          phase: 'attention',
          kind: pending.kind,
          label: 'ATTENTION',
          detail: `${text.detail} The system is still not idle after ${Math.round(ageSec)} s. Check progress, launcher logs, and telemetry; STOP/ABORT remain available.`
        };
      }
      return {phase: 'starting', kind: pending.kind, label: text.label, detail: text.detail};
    }

    const finalProcess = matchingRecentFinalProcess(data || {}, pending);
    if (finalProcess && actual.phase === 'ready') {
      state.pendingOperation = null;
      const rc = finalProcess.returncode;
      const okFinal = rc === 0 || rc === '0';
      if (okFinal) {
        return {phase: 'ready', kind: 'idle', label: 'READY', detail: `${text.label.replace(/\.\.\.$/, '')} finished before the next status update; launcher exited normally.`};
      }
      return {phase: 'attention', kind: pending.kind, label: 'ATTENTION', detail: `${text.label.replace(/\.\.\.$/, '')} launcher ended with status=${finalProcess.status || 'unknown'}, returncode=${rc ?? 'unknown'}. Check launcher logs.`};
    }
    if (pending.kind === 'mount' && actual.phase === 'ready' && mountPendingReached(data || {}, pending)) {
      state.pendingOperation = null;
      return {phase: 'ready', kind: 'idle', label: 'READY', detail: 'Mount command is complete or no motion was needed; Current Az/El matches the requested target.'};
    }
    if (actual.phase === 'running') {
      if (pending.kind === actual.kind || pending.kind === 'observation' || pending.kind === 'rsky' || pending.kind === 'skydip' || pending.kind === 'mount' || pending.kind === 'tracking') {
        state.pendingOperation = null;
      }
      return actual;
    }
    if (ageSec > PENDING_ATTENTION_SEC) {
      return {
        phase: 'attention',
        kind: pending.kind,
        label: 'ATTENTION',
        detail: pending.kind === 'mount'
          ? `${text.detail} No motion confirmation after ${Math.round(ageSec)} s. The mount may already be at the target or the motion may have finished quickly; check Current Az/El, telemetry, or use STOP if needed.`
          : `${text.detail} No confirmation after ${Math.round(ageSec)} s. Check telemetry, progress, or use STOP/ABORT if needed.`
      };
    }
    return {phase: 'starting', kind: pending.kind, label: text.label, detail: text.detail};
  }
  return actual;
}
function beginPendingOperation(kind, action, details={}) {
  state.pendingOperation = Object.assign({kind, action, startedAt: Date.now()}, details || {});
  const op = deriveOperationState(state.lastStatus || {});
  applyOperationUi(op);
}
function clearPendingOperation() {
  state.pendingOperation = null;
  applyOperationUi(deriveOperationState(state.lastStatus || {}));
}
function setCardState(id, mode) {
  const el = qs(id);
  if (!el) return;
  el.classList.remove('op-starting', 'op-running', 'op-attention');
  if (mode && mode !== 'ready') el.classList.add('op-' + mode);
}
function setButtonText(id, text) {
  const el = qs(id);
  if (el) el.textContent = text;
}
function setButtonBusyClass(id, mode) {
  const el = qs(id);
  if (!el) return;
  el.classList.remove('busy', 'pending');
  if (mode === 'running') el.classList.add('busy');
  if (mode === 'starting') el.classList.add('pending');
}
function setActionButtonCurrent(id, active, currentText, normalText) {
  const el = qs(id);
  if (!el) return;
  el.classList.toggle('primary', Boolean(active));
  el.classList.toggle('secondary', !active);
  el.classList.toggle('selected', Boolean(active));
  el.setAttribute('aria-pressed', active ? 'true' : 'false');
  if (currentText || normalText) el.textContent = active ? currentText : normalText;
}
function normalizeChopperState(value) {
  return String(value ?? '').trim().toUpperCase().replace(/[?！!]+$/g, '');
}
function updateChopperButtons(chopper) {
  const stateText = normalizeChopperState(chopper && chopper.state);
  const isIn = stateText === 'IN';
  const isOut = stateText === 'OUT';
  setActionButtonCurrent('chopperIn', isIn, 'Chopper IN (current)', 'Chopper IN');
  setActionButtonCurrent('chopperOut', isOut, 'Chopper OUT (current)', 'Chopper OUT');
  const inButton = qs('chopperIn');
  const outButton = qs('chopperOut');
  if (inButton) inButton.title = isIn ? 'Current chopper state is IN.' : 'Move chopper to IN.';
  if (outButton) outButton.title = isOut ? 'Current chopper state is OUT.' : 'Move chopper to OUT.';
}

function basenameFromPath(path) {
  const s = String(path || '').trim().replace(/\/+$/, '');
  if (!s) return '';
  const idx = s.lastIndexOf('/');
  return idx >= 0 ? s.slice(idx + 1) : s;
}
function observationOutputInfo(data) {
  const obs = (data && data.observation) || {};
  const record = String(obs.record_name || '').trim();
  const containerDir = String(obs.recording_dir || '').trim();
  const localDir = String(obs.local_recording_dir || obs.local_data_dir || '').trim();
  const progressDir = String(obs.progress_record_dir || '').trim();
  const mode = String(obs.record_path_display_mode || 'local').trim().toLowerCase();
  const preferContainer = mode === 'container' || mode === 'docker' || mode === 'internal';
  const directoryName = record || basenameFromPath(localDir) || basenameFromPath(containerDir);
  const displayPath = preferContainer ? (containerDir || localDir) : (localDir || containerDir);
  const displayKind = preferContainer ? 'container' : 'local';
  return {directoryName, record, containerDir, localDir, progressDir, displayPath, displayKind};
}
function updateObservationOutputBox(data) {
  const box = qs('obsOutputDirBox');
  const nameEl = qs('obsOutputDirName');
  const metaEl = qs('obsOutputDirMeta');
  const copyBtn = qs('copyObsOutputDir');
  if (!box || !nameEl || !metaEl) return;
  const info = observationOutputInfo(data || {});
  const hasName = Boolean(info.directoryName);
  box.classList.toggle('visible', hasName);
  nameEl.textContent = hasName ? info.directoryName : 'not available yet';
  nameEl.title = hasName ? 'Directory name copied by the Copy button.' : 'Observation data directory is not available yet.';
  if (hasName) {
    const label = info.displayKind === 'container' ? 'record (container)' : 'record';
    metaEl.innerHTML = info.displayPath ? `${escapeHtml(label)}: <code>${escapeHtml(info.displayPath)}</code>` : 'record: path not available yet';
  } else {
    metaEl.textContent = 'Shown when the console knows the latest record name.';
  }
  if (copyBtn) {
    copyBtn.disabled = !hasName;
    copyBtn.title = hasName ? 'Copy the directory name for analysis notebooks.' : 'No observation data directory is available yet.';
  }
}
async function copyObservationOutputDir() {
  const info = observationOutputInfo(state.lastStatus || {});
  const value = info.directoryName;
  if (!value) {
    log(false, 'No observation data directory name is available to copy.');
    return;
  }
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(value);
      log(true, `Copied observation directory name: ${value}`);
      const btn = qs('copyObsOutputDir');
      if (btn) {
        const old = btn.textContent;
        btn.textContent = 'Copied';
        setTimeout(() => { btn.textContent = old || 'Copy'; }, 1200);
      }
      return;
    }
  } catch (err) {}
  window.prompt('Copy observation data directory name', value);
}
function progressMonitorSnapshot(data) {
  const progress = (data && data.progress) || {};
  const monitor = progress.monitor || progress;
  const status = String(monitor.status || progress.monitor_status || '').trim().toLowerCase();
  const url = String(monitor.progress_url || monitor.url || progress.url || data.progress_url || '').trim();
  const running = Boolean(monitor.running || progress.running);
  const readyStatus = !status || ['managed', 'external', 'running', 'ready', 'ok'].includes(status);
  const starting = ['managed_starting', 'starting', 'pending'].includes(status);
  const failed = ['exited', 'failed', 'error'].includes(status) || (monitor.returncode !== null && monitor.returncode !== undefined && monitor.returncode !== 0);
  return {running, ready: running && readyStatus, starting, failed, status, url, message: monitor.message || progress.message || ''};
}
function updateProgressLaunchButton(data) {
  const btn = qs('openProgress');
  if (!btn) return;
  const snap = progressMonitorSnapshot(data || state.lastStatus || {});
  if (state.progressLaunch.active) {
    btn.textContent = 'Starting progress...';
    btn.disabled = true;
    btn.classList.add('selected');
    btn.title = 'Waiting for the progress monitor server to become reachable.';
    return;
  }
  btn.disabled = false;
  btn.classList.remove('selected');
  if (snap.ready) {
    btn.textContent = 'Open progress monitor';
    btn.title = `Progress monitor is reachable: ${snap.url || 'URL unknown'}`;
  } else if (snap.starting) {
    btn.textContent = 'Progress starting...';
    btn.title = snap.message || 'Progress monitor process is starting; click to retry opening when ready.';
  } else if (snap.failed) {
    btn.textContent = 'Retry progress monitor';
    btn.title = snap.message || 'Progress monitor failed or exited; click to retry launch.';
  } else {
    btn.textContent = 'Launch progress monitor';
    btn.title = 'Start or open the progress-monitor server managed by this console. If this console exits, only a console-owned progress server stops.';
  }
}
function setProgressLaunchMessage(kind, text) {
  const el = qs('runtimeProgressMonitor');
  if (el) el.textContent = text;
  const btn = qs('openProgress');
  if (btn) btn.title = text;
  if (kind === 'bad') log(false, text);
  else if (kind === 'ok') log(true, text);
}
async function waitForProgressReady(initialUrl) {
  const timeoutMs = 12000;
  const started = Date.now();
  let lastUrl = initialUrl || '';
  let lastMessage = 'Waiting for progress server...';
  state.progressLaunch.active = true;
  updateProgressLaunchButton(state.lastStatus || {});
  try {
    while (Date.now() - started < timeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 350));
      try { await refresh(); } catch (_) {}
      const snap = progressMonitorSnapshot(state.lastStatus || {});
      if (snap.url) lastUrl = snap.url;
      if (snap.message) lastMessage = snap.message;
      if (snap.ready && lastUrl) {
        state.progressLaunch.active = false;
        updateProgressLaunchButton(state.lastStatus || {});
        setProgressLaunchMessage('ok', 'Progress monitor is ready. Opening it now.');
        window.open(lastUrl, '_blank');
        return true;
      }
      if (snap.failed && !snap.starting) break;
    }
  } finally {
    state.progressLaunch.active = false;
    updateProgressLaunchButton(state.lastStatus || {});
  }
  setProgressLaunchMessage('bad', `Progress monitor is not reachable yet. ${lastMessage}. Try Open/Retry or inspect progress logs.`);
  if (lastUrl) window.open(lastUrl, '_blank');
  return false;
}
async function launchOrOpenProgress() {
  const existing = progressMonitorSnapshot(state.lastStatus || {});
  if (existing.ready && existing.url) {
    window.open(existing.url, '_blank');
    return;
  }
  state.progressLaunch.active = true;
  updateProgressLaunchButton(state.lastStatus || {});
  const data = await api('launch_progress');
  const url = data.progress_url || data.url || (data.state && data.state.progress_url) || existing.url;
  if (!data.ok) {
    state.progressLaunch.active = false;
    updateProgressLaunchButton(state.lastStatus || {});
    setProgressLaunchMessage('bad', data.reason || data.message || 'Failed to launch progress monitor.');
    return;
  }
  await waitForProgressReady(url);
}
function operationLocksActive(op) {
  return Boolean(op && ['starting', 'running', 'attention'].includes(op.phase));
}
function applyOperationUi(op) {
  op = op || {phase: 'ready', kind: 'idle', label: 'READY', detail: ''};
  state.currentOperation = op;
  const banner = qs('systemBanner');
  if (banner) {
    banner.classList.remove('ready', 'starting', 'running', 'attention');
    banner.classList.add(op.phase === 'ready' ? 'ready' : op.phase);
  }
  setText('systemStateLabel', op.label || 'READY');
  setText('systemStateDetail', op.detail || '');
  const isObs = op.kind === 'observation';
  const isMount = op.kind === 'mount';
  const isCal = ['rsky', 'skydip', 'calibration'].includes(op.kind);
  setCardState('observationCard', isObs ? op.phase : 'ready');
  setCardState('manualCard', (isMount || isCal || op.kind === 'tracking') ? op.phase : 'ready');
  setCardState('rskyCard', (op.kind === 'rsky' || op.kind === 'calibration') ? op.phase : 'ready');
  setCardState('skydipCard', (op.kind === 'skydip' || op.kind === 'calibration') ? op.phase : 'ready');
  setButtonText('startObs', isObs ? (op.phase === 'running' ? 'Running...' : (op.phase === 'starting' ? 'Starting...' : (op.phase === 'attention' ? 'Attention' : 'Start observation'))) : 'Start observation');
  setButtonText('moveMount', isMount ? (op.phase === 'running' ? 'Moving...' : (op.phase === 'starting' ? 'Command sent...' : (op.phase === 'attention' ? 'Check mount' : 'Move mount'))) : 'Move mount');
  setButtonText('runRsky', op.kind === 'rsky' ? (op.phase === 'running' ? 'RSky running...' : (op.phase === 'starting' ? 'Starting RSky...' : (op.phase === 'attention' ? 'Attention' : 'Run RSky'))) : 'Run RSky');
  setButtonText('runSkydip', op.kind === 'skydip' ? (op.phase === 'running' ? 'SkyDip running...' : (op.phase === 'starting' ? 'Starting SkyDip...' : (op.phase === 'attention' ? 'Attention' : 'Run SkyDip'))) : 'Run SkyDip');
  setButtonBusyClass('startObs', isObs ? op.phase : 'ready');
  setButtonBusyClass('moveMount', isMount ? op.phase : 'ready');
  setButtonBusyClass('runRsky', op.kind === 'rsky' ? op.phase : 'ready');
  setButtonBusyClass('runSkydip', op.kind === 'skydip' ? op.phase : 'ready');
  enforceOperationLocks();
}
function enforceOperationLocks() {
  const op = state.currentOperation || {phase: 'ready', kind: 'idle'};
  const locked = operationLocksActive(op);
  const lockIds = [
    'startObs', 'checkObs', 'dryRunObs', 'openObsChooser', 'previewCurrentObs',
    'recentDir', 'obsFilename', 'obsMode', 'obsChannel', 'obsPreview', 'obsFile',
    'moveMount', 'dryRunMount', 'startTracking', 'runRsky', 'runSkydip',
    'rskyN', 'rskyInteg', 'rskyCh', 'skydipInteg', 'skydipCh', 'skydipTpRange'
  ];
  const formOnlyIds = [
    'openObsChooser', 'previewCurrentObs', 'recentDir', 'obsFilename', 'obsMode',
    'obsChannel', 'obsPreview', 'obsFile', 'rskyN', 'rskyInteg', 'rskyCh',
    'skydipInteg', 'skydipCh', 'skydipTpRange'
  ];
  if (locked) {
    lockIds.forEach(id => setDisabled(id, true));
  } else {
    // Validation/capability functions own the actual Start buttons.  Restore
    // only form and chooser controls here so a previous RUNNING lock does not
    // leave the UI stuck after an external STOP/ABORT.
    formOnlyIds.forEach(id => setDisabled(id, false));
  }
  // STOP and ABORT are safety actions and must remain clickable even if status is stale.
  setDisabled('stopButton', false);
  setDisabled('abortObs', false);
}
function capabilityText(caps) {
  if (!caps || typeof caps !== 'object') return 'unknown';
  const enabled = Object.entries(caps).filter(([, v]) => Boolean(v)).map(([k]) => k);
  const disabled = Object.entries(caps).filter(([, v]) => !Boolean(v)).map(([k]) => k);
  const head = enabled.length ? enabled.join(', ') : 'none';
  return disabled.length ? `${head}; disabled: ${disabled.join(', ')}` : head;
}
function processSummary(counts) {
  counts = counts || {};
  return `total ${counts.total ?? 0}, active ${counts.active ?? counts.running ?? 0}, running ${counts.running ?? 0}, terminating ${counts.terminating ?? 0}, exited ${counts.exited ?? 0}, failed ${counts.failed ?? 0}, lost ${counts.lost ?? 0}`;
}
function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[ch]));
}
function renderProcessRows(processes) {
  if (!Array.isArray(processes) || processes.length === 0) return '<span style="color:var(--faint)">No launcher process recorded.</span>';
  return processes.slice(0, 8).map(p => {
    const rc = (p.returncode === null || p.returncode === undefined) ? '' : `, rc=${p.returncode}`;
    const pidValue = p.pid;
    const pid = pidValue === undefined ? 'pid=?' : `pid=${pidValue}`;
    const started = escapeHtml(p.started_at_iso || '');
    const label = escapeHtml(p.label || p.action || 'launcher');
    const status = escapeHtml(p.status || 'unknown');
    const logs = [p.stdout_path ? `stdout: ${escapeHtml(p.stdout_path)}` : '', p.stderr_path ? `stderr: ${escapeHtml(p.stderr_path)}` : ''].filter(Boolean).join('<br>');
    const canTerminate = p.can_terminate || ['running', 'terminating', 'killing'].includes(String(p.status || ''));
    const button = canTerminate && pidValue !== undefined ? `<br><button class="secondary process-stop" data-pid="${escapeHtml(pidValue)}">Terminate local launcher</button>` : '';
    return `<div class="process-row"><b>${label}</b> ${pid}, ${status}${rc}<br><small>${started}</small>${logs ? '<br><small>' + logs + '</small>' : ''}${button}</div>`;
  }).join('');
}
function renderLauncherLogChoices(processes) {
  const el = qs('runtimeLauncherLogChoices');
  if (!el) return;
  const choices = [];
  if (Array.isArray(processes)) {
    processes.slice(0, 8).forEach(p => {
      const label = p.label || p.action || 'launcher';
      if (p.stdout_path) choices.push({label, stream:'stdout', path:p.stdout_path, pid:p.pid});
      if (p.stderr_path) choices.push({label, stream:'stderr', path:p.stderr_path, pid:p.pid});
    });
  }
  if (!choices.length) {
    el.innerHTML = '<span style="color:var(--faint)">No launcher log recorded.</span>';
    return;
  }
  el.innerHTML = choices.slice(0, 12).map(c => `<button class="secondary log-choice" data-path="${escapeHtml(c.path)}">${escapeHtml(c.label)} ${escapeHtml(c.stream)} pid=${escapeHtml(c.pid ?? '?')}</button>`).join('');
}
function renderLogBrowser(payload) {
  const summary = qs('runtimeLogBrowserSummary');
  const body = qs('runtimeLogBrowserText');
  if (!summary || !body) return;
  if (!payload) {
    summary.textContent = 'Operator and launcher logs are read-only.';
    body.textContent = 'Select a log to display its tail.';
    return;
  }
  summary.textContent = payload.reason || (payload.ok ? 'log read' : 'log read failed');
  if (Array.isArray(payload.entries)) {
    body.textContent = payload.entries.map(e => {
      const when = e.time_iso || e.time || '';
      const mark = e.ok === false ? 'NG' : 'OK';
      const action = e.action ? ` ${e.action}` : '';
      const msg = e.message || e.reason || '';
      return `${when} ${mark}${action}: ${msg}`;
    }).join('\n') || '(operator log is empty)';
    return;
  }
  body.textContent = payload.text || '(log is empty)';
}
async function readOperatorLog() {
  const resp = await fetch('/api/operator-log?limit=80');
  const data = await resp.json();
  renderLogBrowser(data);
}
async function readLogFile(path) {
  const resp = await fetch('/api/log-file?max_bytes=32768&path=' + encodeURIComponent(path));
  const data = await resp.json();
  renderLogBrowser(data);
}
function progressMonitorText(progress) {
  const mon = progress && (progress.monitor || progress.monitor_status || progress.progress_monitor);
  if (mon && typeof mon === 'object') {
    const owner = mon.owned_by_console ? 'console-owned' : (mon.running ? 'external/not owned' : 'not running');
    const pid = mon.pid ? ` pid=${mon.pid}` : '';
    return `${mon.status || (mon.running ? 'running' : 'stopped')} (${owner})${pid}`;
  }
  return progress && progress.running ? 'running' : 'not running';
}
function renderSelfCheck(result) {
  const statusEl = qs('runtimeSelfCheckStatus');
  const summaryEl = qs('runtimeSelfCheckSummary');
  if (!statusEl || !summaryEl) return;
  if (!result) {
    statusEl.textContent = 'not run';
    summaryEl.innerHTML = '<span style="color:var(--faint)">Run self-check before live operation.</span>';
    return;
  }
  const summary = result.summary || {};
  const status = result.status || (result.ok ? 'ok' : 'error');
  statusEl.textContent = `${status} (${summary.error_count ?? 0} errors, ${summary.warning_count ?? 0} warnings)`;
  const checks = Array.isArray(result.checks) ? result.checks : [];
  summaryEl.innerHTML = checks.slice(0, 10).map(item => {
    const icon = item.ok ? (item.severity === 'warning' ? '⚠' : '✓') : '✗';
    const cls = item.ok ? (item.severity === 'warning' ? 'warn' : 'ok') : 'bad';
    return `<div><span class="${cls}">${icon}</span> <b>${escapeHtml(item.name)}</b>: ${escapeHtml(item.message)}</div>`;
  }).join('') || '<span style="color:var(--faint)">No self-check details.</span>';
}
function renderRuntime(data) {
  const site = data.site || {};
  const progress = data.progress || {};
  const processes = Array.isArray(data.processes) ? data.processes : [];
  const counts = data.process_counts || {};
  const liveActions = data.live_actions || {};
  setText('runtimeActionMode', data.action_mode || 'unknown');
  setText('runtimeLiveWrites', liveActions.guarded ? 'guarded by CLI option' : (liveActions.enabled ? 'enabled' : 'dry-run / not live'));
  const refreshMs = Number(data.status_refresh_ms || getStatusRefreshMs());
  setText('runtimeStatusRefresh', `${refreshMs} ms`);
  const liveTelemetry = data.live_telemetry || {};
  let liveTelemetryText = 'disabled';
  if (liveTelemetry.requested !== false) {
    if (liveTelemetry.available) {
      const counts = liveTelemetry.sample_counts || {};
      const topics = Object.entries(counts).filter(([, v]) => Number(v) > 0).map(([k, v]) => `${k}:${v}`).slice(0, 5).join(', ');
      const pos = liveTelemetry.has_position ? 'position OK' : 'waiting for position sample';
      liveTelemetryText = `available (${liveTelemetry.spin_mode || 'spin'}, ${pos}${topics ? '; ' + topics : ''})`;
    } else {
      liveTelemetryText = `unavailable${liveTelemetry.error ? ': ' + liveTelemetry.error : ''}`;
    }
  }
  setText('runtimeLiveTelemetry', liveTelemetryText);
  setText('runtimeObservatory', site.observatory || 'unknown');
  setText('runtimeConfigSource', site.source || 'unknown');
  setText('runtimeConfigPath', site.source_path || '(none)');
  setText('runtimeCapabilities', capabilityText(data.capabilities || {}));
  setText('runtimeProcessCounts', processSummary(counts));
  const processEl = qs('runtimeProcesses');
  if (processEl) processEl.innerHTML = renderProcessRows(processes);
  const obs = data.observation || {};
  setText('runtimeRecordName', obs.record_name || '(none)');
  setText('runtimeLocalRecordingDir', obs.local_recording_dir || obs.local_data_dir || obs.recording_dir || '(not available yet)');
  setText('runtimeRecordingDir', obs.recording_dir || '(not available yet)');
  setText('runtimeProgressRecordDir', obs.progress_record_dir || '(not available yet)');
  renderLauncherLogChoices(processes);
  setText('runtimeProgressMonitor', progressMonitorText(progress));
  updateProgressLaunchButton(data);
  setText('runtimeOperatorLog', data.operator_log_path || '(not configured)');
  setText('runtimeLauncherLogDir', data.launcher_log_dir || '(not configured)');
  const shutdown = data.shutdown || {};
  setText('runtimeShutdownStatus', shutdown.status || 'not_started');
  setText('runtimeShutdownTerminate', shutdown.terminate_launchers === false ? 'no' : 'yes');
  setText('runtimeShutdownLast', shutdown.cleanup_finished_at_iso || shutdown.cleanup_started_at_iso || '(none)');
  const shutdownSummary = shutdown.summary || {};
  const shutdownEl = qs('runtimeShutdownSummary');
  if (shutdownEl) {
    const launcher = shutdownSummary.launchers || {};
    const progress = shutdownSummary.progress_monitor || {};
    shutdownEl.innerHTML = [
      launcher.message ? `Launchers: ${escapeHtml(launcher.message)}` : '',
      progress.message ? `Progress: ${escapeHtml(progress.message)}` : ''
    ].filter(Boolean).join('<br>') || '<span style="color:var(--faint)">No shutdown cleanup has run in this console process.</span>';
  }
  renderSelfCheck(state.lastSelfCheck);
  const warnings = Array.isArray(data.warnings) ? data.warnings : [];
  const warnEl = qs('runtimeWarnings');
  if (warnEl) warnEl.innerHTML = warnings.length ? warnings.map(w => `<div>⚠ ${String(w)}</div>`).join('') : '<span style="color:var(--ok)">No warning reported.</span>';
}
function applyCapabilities(data) {
  const caps = data.capabilities || {};
  const liveGuarded = Boolean((data.live_actions || {}).guarded);
  const chopperMoveDisabled = caps.chopper_move === false || liveGuarded;
  setDisabled('chopperIn', chopperMoveDisabled);
  setDisabled('chopperOut', chopperMoveDisabled);
  setDisabled('chopperStatus', caps.chopper_status === false);
  const maintDisabled = caps.chopper_maintenance === false || liveGuarded;
  setDisabled('chopperAlarmReset', maintDisabled);
  setDisabled('chopperHome', maintDisabled);
  setDisabled('chopperRecover', maintDisabled);
  setDisabled('startTracking', liveGuarded || caps.target_tracking === false || qs('startTracking').disabled);
  setDisabled('runRsky', liveGuarded || caps.rsky === false);
  setDisabled('runSkydip', liveGuarded || caps.skydip === false);
  if (caps.mount_move === false || liveGuarded) {
    setDisabled('moveMount', true);
  }
  if (caps.mount_move === false) {
    setDisabled('dryRunMount', true);
  }
}
async function api(action, params={}, pendingKind=null) {
  if (pendingKind) {
    const pendingDetails = (typeof pendingKind === 'object') ? pendingKind : {kind: pendingKind};
    beginPendingOperation(pendingDetails.kind, action, pendingDetails);
  }
  let data;
  try {
    const resp = await fetch('/api/action', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action, params, session_id: state.sessionId})
    });
    data = await resp.json();
  } catch (err) {
    data = {ok: false, action, reason: String(err && err.message ? err.message : err)};
  }
  state.lastActionResponse = data;
  if (!data.ok || data.dry_run) {
    clearPendingOperation();
  }
  await refresh();
  return data;
}
function renderStatusRefreshError(err) {
  const message = String(err && err.message ? err.message : err);
  const op = {
    phase: 'attention',
    kind: 'status',
    label: 'ATTENTION',
    detail: 'Console status refresh failed. The displayed state may be stale; STOP and ABORT remain available. Error: ' + message
  };
  state.currentOperation = op;
  applyOperationUi(op);
  const badges = qs('statusBadges');
  if (badges) badges.innerHTML = `<span class="badge bad">Status refresh failed</span> <span class="badge warn">display may be stale</span>`;
}
async function refresh() {
  try {
    const resp = await fetch('/api/status');
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    state.lastStatus = data;
    renderStatus(data);
    renderLog(data.log || []);
  } catch (err) {
    renderStatusRefreshError(err);
  }
}
async function loadServerObsRoots() {
  const rootSel = qs('serverObsRoot');
  const info = qs('serverObsInfo');
  if (!rootSel) return;
  try {
    const resp = await fetch('/api/obs-roots');
    const data = await resp.json();
    const roots = Array.isArray(data.roots) ? data.roots : [];
    state.serverObsRoots = roots;
    rootSel.innerHTML = roots.map(r => `<option value="${escapeHtml(r.path)}">${escapeHtml(r.label || r.path)}</option>`).join('');
    setDirectoryDatalist(roots.map(r => r.path));
    if (roots.length) {
      qs('serverObsDir').value = roots[0].path;
      if (!qs('recentDir').value) qs('recentDir').value = roots[0].path;
      if (info) info.textContent = `NECST-side file chooser: ${roots.length} root(s)`;
      await browseServerObs(roots[0].path);
    } else if (info) {
      info.textContent = 'NECST-side file chooser: no readable locations are available.';
    }
  } catch (err) {
    await useDemoObsBrowser(String(err && err.message ? err.message : err));
  }
}
function formatFileSize(size) {
  if (size === null || size === undefined || size === '') return '';
  const n = Number(size);
  if (!Number.isFinite(n)) return '';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

const DEMO_OBS_FILES = {
  '/root/obs': {type: 'directory'},
  '/root/obs/commissioning': {type: 'directory'},
  '/root/obs/commissioning/orion_otf.obs': {type: 'file', text: '# Demo OTF obs file\n[observation]\nmode = "OTF"\nrecord_name = "demo_orion_otf"\n\n[coordinate]\ncoord_sys = "J2000"\n"lambda_on[hms]" = "05:35:17.3"\n"beta_on[dms]" = "-05:23:28"\n'},
  '/root/obs/commissioning/rsky.toml': {type: 'file', text: '# Demo TOML obs/check file\n[observation]\nmode = "PSW"\nrecord_name = "demo_rsky"\n'},
  '/data/observations': {type: 'directory'},
  '/data/observations/current': {type: 'directory'},
  '/data/observations/current/test_grid.obs': {type: 'file', text: '# Demo Grid obs file\n[observation]\nmode = "Grid"\nrecord_name = "demo_grid"\n'}
};
function demoObsRootsPayload() {
  return [
    {path: '/root/obs', label: 'Demo obs folder (/root/obs)'},
    {path: '/data/observations', label: 'Demo observations (/data/observations)'},
    {path: '/', label: 'Demo filesystem root (/)'}
  ];
}
function demoNormalizePath(path) {
  let p = String(path || '/root/obs').trim() || '/root/obs';
  if (!p.startsWith('/')) p = '/root/obs/' + p;
  p = p.replace(/\/+/g, '/');
  if (p.length > 1) p = p.replace(/\/+$/, '');
  return p || '/';
}
function demoListDir(dir) {
  const d = demoNormalizePath(dir);
  const entries = [];
  if (d !== '/') entries.push({type: 'directory', name: '..', path: dirnameOf(d), size: null});
  const seen = new Set();
  for (const [path, node] of Object.entries(DEMO_OBS_FILES)) {
    if (path === d) continue;
    if (!path.startsWith(d === '/' ? '/' : d + '/')) continue;
    const rest = path.slice(d === '/' ? 1 : d.length + 1);
    if (!rest || rest.includes('/')) continue;
    const name = rest;
    if (seen.has(name)) continue;
    seen.add(name);
    entries.push({type: node.type, name, path, size: node.type === 'file' ? (node.text || '').length : null});
  }
  entries.sort((a, b) => (a.type === b.type ? a.name.localeCompare(b.name) : (a.type === 'directory' ? -1 : 1)));
  return {ok: true, directory: d, entries, root_count: 3};
}
function demoPreviewFile(path) {
  const p = demoNormalizePath(path);
  const node = DEMO_OBS_FILES[p];
  if (!node || node.type !== 'file') throw new Error('demo file not found: ' + p);
  const text = node.text || '';
  return {ok: true, path: p, directory: dirnameOf(p), filename: basenameOf(p), text, size_bytes: text.length, returned_bytes: text.length, line_count: text.split('\n').length, truncated: false};
}
async function useDemoObsBrowser(reason) {
  state.demoObsBrowser = true;
  const roots = demoObsRootsPayload();
  const rootSel = qs('serverObsRoot');
  const info = qs('serverObsInfo');
  state.serverObsRoots = roots;
  rootSel.innerHTML = roots.map(r => `<option value="${escapeHtml(r.path)}">${escapeHtml(r.label || r.path)}</option>`).join('');
  setDirectoryDatalist(roots.map(r => r.path));
  qs('serverObsDir').value = roots[0].path;
  if (!qs('recentDir').value) qs('recentDir').value = roots[0].path;
  if (info) info.textContent = 'Demo NECST-side file chooser shown because the live API is unavailable' + (reason ? ` (${reason})` : '') + '.';
  await browseServerObs(roots[0].path);
}
function renderServerObsEntries(entries, directory) {
  const list = qs('serverObsFiles');
  if (!list) return;
  const safeEntries = Array.isArray(entries) ? entries : [];
  if (!safeEntries.length) {
    list.innerHTML = '<div class="file-empty">No .obs/.toml files or subfolders in this folder.</div>';
    return;
  }
  list.innerHTML = safeEntries.map((e, idx) => {
    const type = e.type === 'directory' ? 'directory' : 'file';
    const icon = type === 'directory' ? (e.name === '..' ? '↩' : '📁') : '📄';
    const meta = type === 'directory' ? 'folder' : formatFileSize(e.size);
    const name = e.name === '..' ? '.. parent folder' : e.name;
    const selected = e.path && e.path === state.selectedObsPath ? ' selected' : '';
    return `<button type="button" class="file-row ${type}${selected}" role="option" data-index="${idx}" data-type="${escapeHtml(type)}" data-path="${escapeHtml(e.path || '')}" title="${escapeHtml(e.path || '')}">` +
      `<span class="file-icon">${icon}</span>` +
      `<span class="file-name">${escapeHtml(name || '')}</span>` +
      `<span class="file-meta">${escapeHtml(meta || '')}</span>` +
      `</button>`;
  }).join('');
}
async function browseServerObs(dir) {
  const info = qs('serverObsInfo');
  const list = qs('serverObsFiles');
  if (!list) return;
  const browseDir = String(dir || qs('serverObsDir').value || qs('serverObsRoot').value || '').trim();
  if (!browseDir) {
    if (info) info.textContent = 'NECST-side file chooser: choose a root or folder.';
    list.innerHTML = '<div class="file-empty">No readable NECST-side location is available.</div>';
    return;
  }
  try {
    if (info) info.textContent = 'NECST-side file chooser: loading...';
    let data;
    if (state.demoObsBrowser) {
      data = demoListDir(browseDir);
    } else {
      const resp = await fetch('/api/obs-list?dir=' + encodeURIComponent(browseDir));
      data = await resp.json();
    }
    if (!data.ok) throw new Error(data.reason || 'obs list failed');
    const entries = Array.isArray(data.entries) ? data.entries : [];
    qs('serverObsDir').value = data.directory || browseDir;
    if (!qs('recentDir').value) qs('recentDir').value = data.directory || browseDir;
    setDirectoryDatalist([data.directory, ...(state.serverObsRoots || []).map(r => r.path)]);
    renderServerObsEntries(entries, data.directory || browseDir);
    if (info) info.textContent = `${state.demoObsBrowser ? 'Demo ' : ''}NECST-side file chooser: ${entries.length} item(s) in ${data.directory || browseDir}`;
  } catch (err) {
    if (!state.demoObsBrowser) {
      await useDemoObsBrowser(String(err && err.message ? err.message : err));
      return;
    }
    list.innerHTML = '<div class="file-empty">Cannot open this folder. Check the path.</div>';
    if (info) info.textContent = 'NECST-side file chooser error: ' + err;
  }
}
async function previewServerObsFile(path) {
  const info = qs('previewInfo');
  try {
    let data;
    if (state.demoObsBrowser) {
      data = demoPreviewFile(path);
    } else {
      const resp = await fetch('/api/obs-preview?path=' + encodeURIComponent(path) + '&max_bytes=65536');
      data = await resp.json();
    }
    if (!data.ok) throw new Error(data.reason || 'preview failed');
    state.selectedObsPath = data.path || path;
    const useButton = qs('useSelectedObs');
    if (useButton) useButton.disabled = false;
    setObsPathFromServerPath(data.path || path);
    qs('obsPreview').value = data.text || '';
    updatePreviewInfo();
    if (info) {
      const trunc = data.truncated ? ', truncated' : '';
      info.textContent = `${state.demoObsBrowser ? 'Demo preview' : 'NECST-side preview'}: ${data.line_count || 0} lines, ${data.size_bytes || 0} bytes${trunc}`;
    }
  } catch (err) {
    if (info) info.textContent = 'NECST-side preview failed: ' + err;
  }
}

function openObsChooser() {
  const modal = qs('obsChooserModal');
  if (!modal) return;
  modal.classList.add('open');
  document.body.style.overflow = 'hidden';
  const current = qs('obsPath').value.trim() || qs('serverObsDir').value.trim() || (qs('serverObsRoot') ? qs('serverObsRoot').value : '');
  if (current) {
    const dir = current.match(/\.(obs|toml)$/i) ? dirnameOf(current) : current;
    browseServerObs(dir);
  } else {
    loadServerObsRoots();
  }
  setTimeout(() => { try { qs('serverObsDir').focus(); } catch (_) {} }, 0);
}
function closeObsChooser() {
  const modal = qs('obsChooserModal');
  if (!modal) return;
  modal.classList.remove('open');
  document.body.style.overflow = '';
}

function renderStatus(data) {
  state.lastStatus = data || {};
  const operation = deriveOperationState(state.lastStatus);
  // Store the freshly derived state before validation functions run, so a
  // just-finished external STOP/ABORT does not leave controls disabled by the
  // previous RUNNING lock.
  state.currentOperation = operation;
  const heldByMe = data.authority && data.authority.session_id === state.sessionId;
  state.siteLimits = data.mount_limits || state.siteLimits;
  state.capabilities = data.capabilities || state.capabilities || {};
  state.liveActions = data.live_actions || state.liveActions || {guarded: false, enabled: false};
  const site = data.site || {};
  const siteName = site.observatory || 'unknown site';
  const siteSource = site.source || 'unknown source';
  qs('limitSummary').textContent = `${siteName} ${siteSource}: Az ${state.siteLimits.az_min}..${state.siteLimits.az_max} deg, El ${state.siteLimits.el_min}..${state.siteLimits.el_max} deg`;
  const authText = data.authority && data.authority.held ? (heldByMe ? 'Authority: held here' : 'Authority: held elsewhere') : 'Authority: auto';
  const authClass = data.authority && data.authority.held ? (heldByMe ? 'ok' : 'warn') : 'info';
  const taskClass = data.active_task && data.active_task !== 'none' ? 'ok' : 'info';
  const motionAtTargetIdle = Boolean((data.mount_target_reached || data.mount_hold_at_target) && data.motion_live_active === false);
  const motionClass = (!motionAtTargetIdle && (data.manual_state === 'moving' || data.manual_state === 'tracking' || data.manual_state === 'calibration' || data.manual_state === 'observing sequence')) ? 'ok' : (data.manual_state === 'stopped' ? 'warn' : 'info');
  const cmd = (finiteStatusNumber(data.command_az) && finiteStatusNumber(data.command_el)) ? `${Number(data.command_az).toFixed(4)} / ${Number(data.command_el).toFixed(4)}` : '- / -';
  const progressClass = data.progress && data.progress.running ? 'ok' : 'info';
  const progressText = data.progress && data.progress.running ? 'Progress: running' : 'Progress: not started';
  const counts = data.process_counts || {};
  const runningCount = Number(counts.active ?? counts.running ?? 0);
  const processClass = runningCount > 0 ? 'ok' : 'info';
  qs('statusBadges').innerHTML = [
    `<span class="badge info">${data.telescope}</span>`,
    `<span class="badge ${data.action_mode === 'dry-run' ? 'warn' : (state.liveActions.guarded ? 'warn' : 'ok')}">Mode: ${data.action_mode || 'unknown'}${state.liveActions.guarded ? ' guarded' : ''}</span>`,
    `<span class="badge info">Site: ${siteName}</span>`,
    `<span class="badge ${data.state === 'observing' || data.state === 'calibrating' ? 'ok' : 'info'}">System: ${data.state}</span>`,
    `<span class="badge ${motionClass}">Motion: ${data.manual_state}</span>`,
    `<span class="badge ${taskClass}">Active: ${data.active_task || 'none'}</span>`,
    `<span class="badge ${authClass}">${authText}</span>`,
    `<span class="badge ${progressClass}">${progressText}</span>`,
    `<span class="badge ${processClass}">Launchers: ${runningCount} active</span>`,
    `<span class="badge ${data.warning_count ? 'warn' : 'ok'}">Warnings: ${data.warning_count || 0}</span>`
  ].join('');
  qs('authorityButton').textContent = heldByMe ? 'Release authority' : 'Acquire authority';
  qs('authorityButton').disabled = Boolean(state.liveActions.guarded) && !heldByMe;
  qs('obsStateBadge').textContent = operation.kind === 'observation' ? operation.label : data.state;
  qs('obsStateBadge').className = 'badge ' + (operation.kind === 'observation' ? (operation.phase === 'attention' ? 'bad' : (operation.phase === 'running' ? 'warn' : 'info')) : (data.state === 'observing' ? 'ok' : (data.state === 'calibrating' ? 'warn' : 'info')));
  // ABORT is a safety action.  It must stay clickable even when the
  // observation state cannot be inferred from status/progress yet.
  qs('abortObs').disabled = false;
  const staleObs = data.stale_observation || {};
  const staleButton = qs('clearStaleObs');
  if (staleButton) {
    const canClearStale = Boolean(staleObs.can_clear || staleObs.running_stale || operation.label === 'ATTENTION: STALE OBSERVATION STATE');
    staleButton.disabled = !canClearStale;
    staleButton.style.display = canClearStale ? '' : 'none';
    if (canClearStale) {
      const record = staleObs.record_name || 'unknown record';
      staleButton.title = `Clear stale console/progress state for ${record}. Confirm hardware is safe first. This does not send hardware commands or delete data.`;
    }
  }
  qs('manualBadge').textContent = ['mount', 'tracking', 'rsky', 'skydip', 'calibration'].includes(operation.kind) ? operation.label : data.manual_state;
  qs('manualBadge').className = 'badge ' + (operation.phase === 'attention' ? 'bad' : (operation.phase === 'running' ? 'warn' : motionClass));
  qs('motionSummary').textContent = data.manual_state;
  qs('taskSummary').textContent = data.active_task || 'none';
  qs('posSummary').textContent = `${formatMaybeNumber(data.az)} / ${formatMaybeNumber(data.el)}`;
  qs('cmdSummary').textContent = cmd;
  qs('chopperSummary').textContent = `${data.chopper.state} / pos ${data.chopper.position}`;
  qs('chopperState').textContent = data.chopper.state;
  qs('chopperPos').textContent = data.chopper.position;
  qs('chopperAge').textContent = data.chopper.age || 'live demo';
  updateChopperButtons(data.chopper || {});
  updateObservationOutputBox(data);
  qs('stopTracking').disabled = data.manual_state !== 'tracking';
  if (['rsky', 'skydip', 'calibration'].includes(operation.kind)) {
    qs('calStatusNotice').textContent = operation.label + ': ' + operation.detail;
    qs('calStatusNotice').className = 'notice ' + (operation.phase === 'attention' ? 'bad' : 'warn');
  } else {
    qs('calStatusNotice').textContent = `Calibration state: standby. Current active task: ${data.active_task || 'none'}`;
    qs('calStatusNotice').className = 'notice';
  }
  renderRuntime(data);
  applyCapabilities(data);
  // Capabilities/live guard arrive via /api/status after page load.  Re-run
  // validation so Start buttons do not remain disabled until a user edits a field.
  validateObs();
  validateMount();
  validateTarget();
  applyOperationUi(operation);
}
function renderLog(items) {
  qs('log').innerHTML = items.map(item => {
    const statusClass = item.ok ? 'ok' : 'bad';
    return `<div class="log-entry"><span class="time">${item.time}</span> <span class="${statusClass}">${item.ok ? 'OK' : 'NG'}</span> ${item.message}</div>`;
  }).join('') || '<span style="color:var(--faint)">No operation yet.</span>';
}
function dirnameOf(path) {
  const s = String(path || '').trim();
  const idx = s.lastIndexOf('/');
  if (idx <= 0) return idx === 0 ? '/' : '';
  return s.slice(0, idx);
}
function basenameOf(path) {
  const s = String(path || '').trim();
  const idx = s.lastIndexOf('/');
  return idx >= 0 ? s.slice(idx + 1) : s;
}
function setDirectoryDatalist(dirs) {
  const dl = qs('recentDirList');
  if (!dl) return;
  const unique = Array.from(new Set((dirs || []).filter(Boolean)));
  dl.innerHTML = unique.map(d => `<option value="${escapeHtml(d)}"></option>`).join('');
}
function setObsPathFromServerPath(path) {
  const p = String(path || '').trim();
  if (!p) return;
  qs('recentDir').value = dirnameOf(p);
  qs('obsFilename').value = basenameOf(p);
  saveFormField('recentDir');
  saveFormField('obsFilename');
  if (qs('serverObsDir')) qs('serverObsDir').value = dirnameOf(p);
  state.obsChecked = false;
  validateObs();
}
function updateObsRunPath() {
  const nameRaw = qs('obsFilename').value.trim();
  if (nameRaw.startsWith('/')) {
    qs('obsPath').value = nameRaw;
  } else {
    const dir = qs('recentDir').value.trim().replace(/\/+$/, '');
    const name = nameRaw.replace(/^\/+/, '');
    qs('obsPath').value = (dir && name) ? `${dir}/${name}` : '';
  }
  const fullPath = qs('obsPath').value || '';
  qs('obsPath').title = fullPath || 'Read-only computed NECST-side path used by Start observation.';
  const fullPathEl = qs('obsPathFull');
  if (fullPathEl) {
    fullPathEl.textContent = fullPath || 'No obs file selected';
    fullPathEl.title = fullPath || 'Full NECST-side path used by Preview, Check, Dry run, and Start.';
  }
  qs('runBarTitle').textContent = fullPath || 'No obs file selected';
  qs('runBarSub').textContent = fullPath ? `${qs('obsMode').value.toUpperCase()} / ${state.obsChecked ? 'checked' : 'unchecked, Start will validate'}` : 'Set the NECST-side directory and filename.';
}
function updatePreviewInfo() {
  const text = qs('obsPreview').value;
  const lines = text ? text.split(/\r?\n/).length : 0;
  const bytes = new Blob([text]).size;
  qs('previewInfo').textContent = text ? `Preview: ${lines} lines, ${bytes} bytes` : 'Preview: empty';
}
function validateObs() {
  updateObsRunPath();
  updatePreviewInfo();
  const path = qs('obsPath').value.trim();
  const hasPreview = qs('obsPreview').value.trim().length > 0;
  const channel = qs('obsChannel').value.trim();
  let reasons = [];
  if (!path) reasons.push('obs file path/name is empty');
  if (path && !/\.(obs|toml)$/i.test(path)) reasons.push('extension should be .obs or .toml');
  if (channel) {
    const ch = Number(channel);
    if (!Number.isInteger(ch) || ch <= 0) reasons.push('channel override must be a positive integer');
  }
  if (reasons.length) {
    setNotice(qs('obsValidation'), 'bad', 'Cannot start: ' + reasons.join(' / '));
    qs('checkObs').disabled = !path;
    qs('dryRunObs').disabled = true;
    qs('startObs').disabled = true;
    state.obsChecked = false;
    updateObsRunPath();
    enforceOperationLocks();
    return false;
  }
  qs('checkObs').disabled = false;
  qs('dryRunObs').disabled = false;
  const liveGuarded = Boolean(state.liveActions && state.liveActions.guarded);
  const startCapable = state.capabilities.observation_start !== false && !liveGuarded;
  qs('startObs').disabled = !startCapable;
  const checkText = state.obsChecked ? 'Checked. ' : '';
  const previewText = hasPreview ? 'Preview loaded. ' : 'No preview loaded. ';
  const startText = liveGuarded ? 'Live write guard is active; Start is disabled. Use --action-mode dry-run for normal validation, or restart without --guard-live-actions for live operation.' : (startCapable ? 'Start is enabled; Check and Dry run are optional. Start will validate again on the server.' : 'Observation start is disabled by site capability.');
  setNotice(qs('obsValidation'), startCapable ? (state.obsChecked ? 'ok' : 'warn') : 'bad', checkText + previewText + startText);
  updateObsRunPath();
  enforceOperationLocks();
  return true;
}
function validateMount() {
  const az = numberOrNaN(qs('mountAz').value);
  const el = numberOrNaN(qs('mountEl').value);
  const azMin = Number(state.siteLimits.az_min);
  const azMax = Number(state.siteLimits.az_max);
  const elMin = Number(state.siteLimits.el_min);
  const elMax = Number(state.siteLimits.el_max);
  let reasons = [];
  if (!finite(az)) reasons.push('Az is not a number');
  if (!finite(el)) reasons.push('El is not a number');
  if (![azMin, azMax, elMin, elMax].every(finite)) reasons.push('site TOML limits are not available');
  if (finite(azMin) && finite(azMax) && azMin >= azMax) reasons.push('site TOML Az min/max are invalid');
  if (finite(elMin) && finite(elMax) && elMin >= elMax) reasons.push('site TOML El min/max are invalid');
  if (finite(az) && finite(azMin) && finite(azMax) && (az < azMin || az > azMax)) reasons.push(`Az=${az} deg is outside site TOML range ${azMin}..${azMax} deg`);
  if (finite(el) && finite(elMin) && finite(elMax) && (el < elMin || el > elMax)) reasons.push(`El=${el} deg is outside site TOML range ${elMin}..${elMax} deg`);
  if (state.capabilities.mount_move === false) reasons.push('mount move is disabled by site capability');
  markInput(qs('mountAz'), finite(az) && finite(azMin) && finite(azMax) ? (az >= azMin && az <= azMax) : null);
  markInput(qs('mountEl'), finite(el) && finite(elMin) && finite(elMax) ? (el >= elMin && el <= elMax) : null);
  const ok = reasons.length === 0;
  const liveGuarded = Boolean(state.liveActions && state.liveActions.guarded);
  if (ok && liveGuarded) setNotice(qs('mountValidation'), 'warn', `Dry-run is allowed, but live mount move is guarded by --guard-live-actions.`);
  else if (ok) setNotice(qs('mountValidation'), 'ok', `Ready to move: command Az=${az.toFixed(6)} deg, El=${el.toFixed(6)} deg`);
  else setNotice(qs('mountValidation'), 'bad', 'Not movable: ' + reasons.join(' / '));
  qs('moveMount').disabled = !ok || liveGuarded;
  qs('dryRunMount').disabled = !ok;
  enforceOperationLocks();
  return {ok, reasons, az, el};
}
function validateTarget() {
  const kind = qs('targetKind').value;
  const ox = numberOrNaN(qs('offsetX').value);
  const oy = numberOrNaN(qs('offsetY').value);
  let reasons = [];
  if (!finite(ox) || !finite(oy)) reasons.push('offset must be numeric');
  if (finite(ox) && Math.abs(ox) > 36000) reasons.push('Offset X is larger than the demo limit of 36000 arcsec');
  if (finite(oy) && Math.abs(oy) > 36000) reasons.push('Offset Y is larger than the demo limit of 36000 arcsec');
  if (kind === 'name' && !qs('targetName').value.trim()) reasons.push('object name is empty');
  if (kind === 'radec') {
    const ra = parseSexagesimalAngle(qs('coord1').value, true);
    const dec = parseSexagesimalAngle(qs('coord2').value, false);
    if (!ra.ok) reasons.push('RA: ' + ra.reason);
    if (!dec.ok) reasons.push('Dec: ' + dec.reason);
    if (ra.ok && (ra.deg < 0 || ra.deg >= 360)) reasons.push('RA must satisfy 0 <= value < 360 deg');
    if (dec.ok && (dec.deg < -90 || dec.deg > 90)) reasons.push('Dec must satisfy -90..90 deg');
  }
  if (kind === 'galactic') {
    const c1 = numberOrNaN(qs('coord1').value);
    const c2 = numberOrNaN(qs('coord2').value);
    if (!finite(c1) || !finite(c2)) reasons.push('Galactic coordinates must be decimal degrees');
    if (finite(c1) && (c1 < 0 || c1 >= 360)) reasons.push('l must satisfy 0 <= value < 360 deg');
    if (finite(c2) && (c2 < -90 || c2 > 90)) reasons.push('b must satisfy -90..90 deg');
  }
  if (state.capabilities.target_tracking === false) reasons.push('target tracking is disabled by site capability');
  const liveGuarded = Boolean(state.liveActions && state.liveActions.guarded);
  const ok = reasons.length === 0;
  if (ok && liveGuarded) setNotice(qs('targetValidation'), 'warn', 'Target is valid, but live tracking is guarded by --guard-live-actions.');
  else setNotice(qs('targetValidation'), ok ? 'ok' : 'bad', ok ? 'Target tracking can be started. Stop it with STOP.' : 'Cannot track: ' + reasons.join(' / '));
  qs('startTracking').disabled = !ok || liveGuarded;
  enforceOperationLocks();
  return {ok, reasons};
}
function updateTargetFields() {
  const kind = qs('targetKind').value;
  qs('targetNameBlock').style.display = kind === 'name' ? '' : 'none';
  qs('coordBlock').style.display = (kind === 'radec' || kind === 'galactic') ? '' : 'none';
  if (kind === 'radec') { qs('coord1Label').textContent = 'RA [deg or HH:MM:SS]'; qs('coord2Label').textContent = 'Dec [deg or ±DD:MM:SS]'; }
  if (kind === 'galactic') { qs('coord1Label').textContent = 'l [deg]'; qs('coord2Label').textContent = 'b [deg]'; }
  validateTarget();
}
function selectPanel(id) {
  document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.dataset.panel === id));
  document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === id));
}

document.querySelectorAll('.tab').forEach(b => b.addEventListener('click', () => selectPanel(b.dataset.panel)));
['recentDir','obsFilename','obsPreview','obsChannel','obsMode'].forEach(id => qs(id).addEventListener('input', () => { state.obsChecked = false; validateObs(); }));
['mountAz','mountEl'].forEach(id => qs(id).addEventListener('input', validateMount));
['targetKind','targetName','coord1','coord2','offsetFrame','offsetX','offsetY','cosCorrection'].forEach(id => qs(id).addEventListener('input', updateTargetFields));
qs('obsFile').addEventListener('change', async (ev) => {
  const file = ev.target.files[0];
  if (!file) return;
  qs('obsPreview').value = await file.text();
  state.obsChecked = false;
  updatePreviewInfo();
  setNotice(qs('obsValidation'), 'warn', 'Local preview loaded for visual comparison only. Start still uses the NECST-side computed run path.');
});
qs('openObsChooser').addEventListener('click', openObsChooser);
qs('closeObsChooser').addEventListener('click', closeObsChooser);
qs('obsChooserModal').addEventListener('click', (ev) => { if (ev.target === qs('obsChooserModal')) closeObsChooser(); });
document.addEventListener('keydown', (ev) => { if (ev.key === 'Escape' && qs('obsChooserModal').classList.contains('open')) closeObsChooser(); });
qs('previewCurrentObs').addEventListener('click', async () => {
  const path = qs('obsPath').value.trim();
  if (path) await previewServerObsFile(path);
});
qs('useSelectedObs').addEventListener('click', async () => {
  if (!state.selectedObsPath) return;
  await previewServerObsFile(state.selectedObsPath);
  closeObsChooser();
});
qs('serverObsRoot').addEventListener('change', async () => {
  qs('serverObsDir').value = qs('serverObsRoot').value;
  await browseServerObs(qs('serverObsRoot').value);
});
qs('serverObsHome').addEventListener('click', async () => {
  const root = qs('serverObsRoot').value;
  qs('serverObsDir').value = root;
  await browseServerObs(root);
});
qs('serverObsUp').addEventListener('click', async () => {
  const current = qs('serverObsDir').value.trim();
  const parent = dirnameOf(current);
  await browseServerObs(parent || qs('serverObsRoot').value);
});
qs('goServerObsDir').addEventListener('click', async () => { await browseServerObs(qs('serverObsDir').value || qs('serverObsRoot').value); });
qs('serverObsDir').addEventListener('keydown', async (ev) => {
  if (ev.key === 'Enter') {
    ev.preventDefault();
    await browseServerObs(qs('serverObsDir').value || qs('serverObsRoot').value);
  }
});
qs('refreshServerObs').addEventListener('click', async () => { await browseServerObs(qs('serverObsDir').value || qs('serverObsRoot').value); });
qs('serverObsFiles').addEventListener('click', async (ev) => {
  const row = ev.target.closest('.file-row');
  if (!row) return;
  document.querySelectorAll('.file-row.selected').forEach(el => el.classList.remove('selected'));
  row.classList.add('selected');
  const path = row.dataset.path || '';
  const type = row.dataset.type || '';
  if (type === 'directory') {
    qs('serverObsDir').value = path;
    await browseServerObs(path);
  } else {
    state.selectedObsPath = path;
    const useButton = qs('useSelectedObs');
    if (useButton) useButton.disabled = false;
    await previewServerObsFile(path);
    closeObsChooser();
  }
});
qs('serverObsFiles').addEventListener('keydown', async (ev) => {
  const row = ev.target.closest('.file-row');
  if (!row || (ev.key !== 'Enter' && ev.key !== ' ')) return;
  ev.preventDefault();
  row.click();
});
qs('recentDir').addEventListener('change', () => { state.obsChecked = false; validateObs(); });
qs('checkObs').addEventListener('click', async () => {
  if (!validateObs()) return;
  const data = await api('check_observation', {mode: qs('obsMode').value, file: qs('obsPath').value, channel: qs('obsChannel').value, preview: qs('obsPreview').value});
  state.obsChecked = Boolean(data.ok);
  if (data.ok) {
    validateObs();
  } else {
    state.obsChecked = false;
    validateObs();
    setNotice(qs('obsValidation'), 'bad', 'Check failed: ' + (data.reason || 'unknown reason'));
  }
});
qs('dryRunObs').addEventListener('click', async () => {
  if (!validateObs()) return;
  const data = await api('dry_run_observation', {mode: qs('obsMode').value, file: qs('obsPath').value, channel: qs('obsChannel').value, preview: qs('obsPreview').value});
  if (data.ok) {
    setNotice(qs('obsValidation'), 'ok', data.reason || 'Dry run OK. Start observation remains enabled.');
  } else {
    setNotice(qs('obsValidation'), 'bad', 'Dry run failed: ' + (data.reason || 'unknown reason'));
  }
});
qs('startObs').addEventListener('click', async () => {
  if (!validateObs()) return;
  await api('start_observation', {mode: qs('obsMode').value, file: qs('obsPath').value, channel: qs('obsChannel').value}, 'observation');
});
qs('abortObs').addEventListener('click', async () => {
  if (confirm('Abort current observation and request recorder/gate/progress cleanup?')) await api('abort_observation', {}, 'abort');
});
qs('clearStaleObs').addEventListener('click', async () => {
  const stale = (state.lastStatus || {}).stale_observation || {};
  const record = stale.record_name || 'unknown record';
  if (!confirm(`Clear stale observation/progress state for ${record}?\n\nOnly use this after confirming the telescope, recorder, and XFFTS are safe. This does not send hardware commands and does not delete observation data.`)) return;
  clearPendingOperation();
  const resp = await api('clear_stale_observation_state');
  if (!resp.ok) alert('Clear stale state failed: ' + (resp.reason || 'unknown reason'));
});
qs('copyObsOutputDir').addEventListener('click', copyObservationOutputDir);
qs('authorityButton').addEventListener('click', async () => {
  const resp = await fetch('/api/status');
  const data = await resp.json();
  const heldByMe = data.authority && data.authority.session_id === state.sessionId;
  await api(heldByMe ? 'release_authority' : 'acquire_authority');
});
qs('stopButton').addEventListener('click', async () => { await api('stop', {}, 'stop'); });
qs('moveMount').addEventListener('click', async () => {
  const v = validateMount();
  if (!v.ok) return;
  await api('mount_move', {az: qs('mountAz').value, el: qs('mountEl').value}, {kind: 'mount', targetAz: Number(qs('mountAz').value), targetEl: Number(qs('mountEl').value)});
});
qs('dryRunMount').addEventListener('click', async () => {
  const v = validateMount();
  if (!v.ok) return;
  await api('mount_move_dry_run', {az: qs('mountAz').value, el: qs('mountEl').value});
});
qs('startTracking').addEventListener('click', async () => {
  if (!validateTarget().ok) return;
  await api('start_tracking', {
    kind: qs('targetKind').value,
    name: qs('targetName').value,
    coord1: qs('coord1').value,
    coord2: qs('coord2').value,
    offset_frame: qs('offsetFrame').value,
    offset_x_arcsec: qs('offsetX').value,
    offset_y_arcsec: qs('offsetY').value,
    cos_correction: qs('cosCorrection').value
  }, 'tracking');
});
qs('stopTracking').addEventListener('click', async () => { await api('stop_tracking', {}, 'stop'); });
qs('chopperIn').addEventListener('click', () => api('chopper_in'));
qs('chopperOut').addEventListener('click', () => api('chopper_out'));
qs('chopperStatus').addEventListener('click', () => api('chopper_status'));
qs('chopperAlarmReset').addEventListener('click', () => api('chopper_alarm_reset'));
qs('chopperHome').addEventListener('click', () => api('chopper_home'));
qs('chopperRecover').addEventListener('click', () => api('chopper_recover'));
qs('runRsky').addEventListener('click', () => api('run_rsky', {n: qs('rskyN').value, integ: qs('rskyInteg').value, ch: qs('rskyCh').value}, 'rsky'));
qs('runSkydip').addEventListener('click', () => api('run_skydip', {integ: qs('skydipInteg').value, ch: qs('skydipCh').value, tp_range: qs('skydipTpRange').value}, 'skydip'));
qs('clearLog').addEventListener('click', () => api('clear_log'));
qs('refreshRuntime').addEventListener('click', refresh);
qs('loadOperatorLog').addEventListener('click', readOperatorLog);
qs('runtimeLauncherLogChoices').addEventListener('click', async (ev) => {
  const btn = ev.target && ev.target.closest ? ev.target.closest('.log-choice') : null;
  if (!btn) return;
  await readLogFile(btn.dataset.path);
});
qs('runSelfCheck').addEventListener('click', async () => {
  const resp = await fetch('/api/self-check');
  const data = await resp.json();
  state.lastSelfCheck = data;
  renderSelfCheck(data);
  await refresh();
});
qs('terminateObsLauncher').addEventListener('click', async () => { if (confirm('Terminate local observation launcher subprocess only? Telescope STOP is separate.')) await api('terminate_observation_launcher'); });
qs('terminateCalLauncher').addEventListener('click', async () => { if (confirm('Terminate local calibration launcher subprocess only? Telescope STOP is separate.')) await api('terminate_calibration_launcher'); });
qs('terminateAllLaunchers').addEventListener('click', async () => { if (confirm('Terminate all local launcher subprocesses started by this console? Telescope STOP is separate.')) await api('terminate_all_launchers'); });
qs('runtimeProcesses').addEventListener('click', async (ev) => {
  const btn = ev.target && ev.target.closest ? ev.target.closest('.process-stop') : null;
  if (!btn) return;
  const pid = btn.dataset.pid;
  if (confirm(`Terminate local launcher pid=${pid}? Telescope STOP is separate.`)) await api('terminate_process', {pid});
});
qs('openProgress').addEventListener('click', launchOrOpenProgress);
function getStatusRefreshMs() {
  const cfg = window.NECST_CONSOLE_CONFIG || {};
  const raw = Number(cfg.statusRefreshMs ?? cfg.status_refresh_ms ?? 1000);
  if (!Number.isFinite(raw)) return 1000;
  return Math.max(200, Math.round(raw));
}
const statusRefreshMs = getStatusRefreshMs();
setupFormPersistence(); loadServerObsRoots(); validateObs(); updatePreviewInfo(); validateMount(); updateTargetFields(); refresh(); setInterval(refresh, statusRefreshMs);
</script>
</body>
</html>
"""


PROGRESS_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NECST Progress Monitor Demo</title>
<style>
body { margin:0; background:#08101f; color:#eaf0ff; font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
main { max-width:1100px; margin:0 auto; padding:18px; }
.card { border:1px solid #28324a; border-radius:16px; background:#11182c; padding:14px; margin:12px 0; }
.badge { display:inline-block; border:1px solid #365076; border-radius:999px; padding:4px 10px; margin:3px; color:#9fd1ff; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px; }
.kv { color:#aab6d4; }
.kv b { color:#eaf0ff; }
</style>
</head>
<body>
<main>
  <h1>NECST Progress Monitor Demo</h1>
  <p>This is a lightweight placeholder launched and owned by the console demo. No ROS or telescope data are used.</p>
  <div class="card">
    <span class="badge">Lifecycle: idle/demo</span>
    <span class="badge">IRIG-B: demo</span>
    <span class="badge">Recording: demo</span>
    <span class="badge">Weather: demo</span>
  </div>
  <div class="grid">
    <div class="card"><div class="kv">Current item<br><b>none</b></div></div>
    <div class="card"><div class="kv">Antenna<br><b>Az/El from console demo</b></div></div>
    <div class="card"><div class="kv">Spectrometer<br><b>demo status only</b></div></div>
    <div class="card"><div class="kv">Chopper<br><b>demo status only</b></div></div>
  </div>
  <div class="card">
    <b>Lifecycle policy:</b> because this progress monitor was launched by the console demo, it will stop when the console process exits.
  </div>
</main>
</body>
</html>
"""


@dataclass
class LogEntry:
    time: str
    ok: bool
    message: str


@dataclass
class DemoState:
    telescope: str = "OMU1.85m demo"
    progress_url: str = "http://127.0.0.1:8091/"
    progress_running: bool = False
    progress_owned_by_console: bool = False
    state: str = "idle"
    manual_state: str = "idle"
    active_task: str = "none"
    az: float = 180.0
    el: float = 45.0
    command_az: Optional[float] = None
    command_el: Optional[float] = None
    mount_limits: Dict[str, float] = field(default_factory=lambda: {
        "az_min": 5.0,
        "az_max": 355.0,
        "el_min": 5.0,
        "el_max": 85.0,
    })
    warning_count: int = 0
    authority_held: bool = False
    authority_session_id: Optional[str] = None
    chopper_state: str = "OUT"
    chopper_position: int = 19700
    exclusive_start_action: Optional[str] = None
    exclusive_start_started_at: Optional[float] = None
    exclusive_start_message: str = ""
    record_name: Optional[str] = None
    recording_dir: Optional[str] = None
    local_recording_dir: Optional[str] = None
    progress_record_dir: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

    def prune_exclusive_start_guard(self) -> None:
        if self.exclusive_start_started_at is None:
            return
        try:
            age = max(0.0, time.time() - float(self.exclusive_start_started_at))
        except Exception:
            age = 999.0
        if age > EXCLUSIVE_START_STATUS_SEC:
            self.exclusive_start_action = None
            self.exclusive_start_started_at = None
            self.exclusive_start_message = ""

    def to_dict(self) -> Dict[str, Any]:
        self.prune_exclusive_start_guard()
        return {
            "telescope": self.telescope,
            "progress_url": self.progress_url,
            "progress": {
                "url": self.progress_url,
                "running": self.progress_running,
                "owned_by_console": self.progress_owned_by_console,
            },
            "stale_observation": {
                "running_stale": False,
                "can_clear": False,
                "record_name": self.record_name,
            },
            "state": self.state,
            "observation": {
                "record_name": self.record_name,
                "recording_dir": self.recording_dir,
                "local_recording_dir": self.local_recording_dir,
                "progress_record_dir": self.progress_record_dir,
            },
            "manual_state": self.manual_state,
            "active_task": self.active_task,
            "az": self.az,
            "el": self.el,
            "command_az": self.command_az,
            "command_el": self.command_el,
            "mount_limits": self.mount_limits,
            "warning_count": self.warning_count,
            "authority": {
                "held": self.authority_held,
                "session_id": self.authority_session_id,
            },
            "chopper": {
                "state": self.chopper_state,
                "position": self.chopper_position,
                "age": "live demo",
            },
            "exclusive_start_guard": {
                "action": self.exclusive_start_action,
                "started_at": self.exclusive_start_started_at,
                "age_sec": (
                    max(0.0, time.time() - float(self.exclusive_start_started_at))
                    if self.exclusive_start_started_at is not None
                    else None
                ),
                "message": self.exclusive_start_message,
                "guard_sec": EXCLUSIVE_START_STATUS_SEC,
                "blocking_sec": EXCLUSIVE_START_BLOCK_SEC,
            },
            "log": [entry.__dict__ for entry in self.log[-80:]],
        }


class ConsoleDemoServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        request_handler_class: type[BaseHTTPRequestHandler],
        *,
        telescope: str,
        progress_url: str,
        quiet: bool,
        progress_host: str,
        progress_port: int,
    ) -> None:
        super().__init__(server_address, request_handler_class)
        self.state = DemoState(telescope=telescope, progress_url=progress_url)
        self.lock = threading.RLock()
        self.quiet = quiet
        self.progress_host = progress_host
        self.progress_port = int(progress_port)
        self._progress_server: Optional[ThreadingHTTPServer] = None
        self._progress_thread: Optional[threading.Thread] = None
        self.add_log(True, "console demo v7 started; no real telescope command is sent")

    def add_log(self, ok: bool, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.state.log.append(LogEntry(timestamp, ok, message))
        if len(self.state.log) > 200:
            self.state.log = self.state.log[-200:]

    def launch_progress(self) -> Tuple[bool, str, str]:
        if self._progress_server is not None:
            return True, "managed progress monitor is already running", self.state.progress_url
        bind_host = self.progress_host
        display_host = "127.0.0.1" if bind_host in {"", "0.0.0.0"} else bind_host
        try:
            progress_server = ThreadingHTTPServer((bind_host, self.progress_port), ProgressHandler)
        except OSError as exc:
            self.state.progress_running = False
            self.state.progress_owned_by_console = False
            return False, f"failed to start progress monitor demo: {exc}", self.state.progress_url
        progress_server.daemon_threads = True
        thread = threading.Thread(target=progress_server.serve_forever, name="console-demo-progress", daemon=True)
        thread.start()
        self._progress_server = progress_server
        self._progress_thread = thread
        self.state.progress_url = f"http://{display_host}:{self.progress_port}/"
        self.state.progress_running = True
        self.state.progress_owned_by_console = True
        return True, "managed progress monitor demo started", self.state.progress_url

    def stop_progress(self) -> None:
        progress_server = self._progress_server
        progress_thread = self._progress_thread
        self._progress_server = None
        self._progress_thread = None
        if progress_server is not None:
            progress_server.shutdown()
            progress_server.server_close()
        if progress_thread is not None:
            progress_thread.join(timeout=2.0)
        self.state.progress_running = False
        self.state.progress_owned_by_console = False


class ProgressHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802 - stdlib API name
        if self.path not in {"/", "/index.html", "/health"}:
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        if self.path == "/health":
            data = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        data = PROGRESS_HTML.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


class Handler(BaseHTTPRequestHandler):
    server: ConsoleDemoServer

    def log_message(self, fmt: str, *args: Any) -> None:
        if not self.server.quiet:
            super().log_message(fmt, *args)

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self) -> None:
        data = HTML.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 - stdlib API name
        if self.path in {"/", "/index.html"}:
            self._send_html()
            return
        if self.path == "/api/status":
            with self.server.lock:
                self._send_json(self.server.state.to_dict())
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/operator-log":
            with self.server.lock:
                entries = []
                for idx, entry in enumerate(self.server.state.log[-80:], start=1):
                    payload = dict(entry.__dict__)
                    payload["_line"] = idx
                    payload["time_iso"] = payload.get("time")
                    entries.append(payload)
                self._send_json({
                    "ok": True,
                    "reason": "demo operator log read",
                    "path": "demo://operator_console.jsonl",
                    "entries": entries,
                    "returned_count": len(entries),
                })
            return
        if parsed.path == "/api/log-file":
            query = urllib.parse.parse_qs(parsed.query)
            requested = (query.get("path") or ["demo://launcher.log"])[0]
            self._send_json({
                "ok": True,
                "reason": "demo launcher log read",
                "path": requested,
                "size_bytes": 0,
                "returned_bytes": 0,
                "truncated_head": False,
                "text": "This is the standalone console demo. No real launcher log file exists.\n",
            })
            return
        if parsed.path == "/api/self-check":
            with self.server.lock:
                self._send_json({
                    "ok": True,
                    "status": "warning",
                    "summary": {
                        "ok": True,
                        "status": "warning",
                        "error_count": 0,
                        "warning_count": 1,
                        "check_count": 3,
                        "telescope": self.server.state.telescope,
                        "action_mode": "demo",
                    },
                    "checks": [
                        {"name": "demo_mode", "ok": True, "severity": "warning", "message": "standalone demo; no real telescope command is sent", "data": {}},
                        {"name": "layout", "ok": True, "severity": "info", "message": "v7 layout is available", "data": {}},
                        {"name": "progress_demo", "ok": True, "severity": "info", "message": "progress demo can be launched from this console", "data": {"url": self.server.state.progress_url}},
                    ],
                })
            return
        if parsed.path == "/health":
            self._send_json({"ok": True})
            return
        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def do_POST(self) -> None:  # noqa: N802 - stdlib API name
        if self.path != "/api/action":
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = self.rfile.read(length).decode("utf-8")
            payload = json.loads(body or "{}")
        except Exception as exc:
            self._send_json({"ok": False, "reason": f"invalid JSON: {exc}"}, 400)
            return
        action = str(payload.get("action") or "")
        params = payload.get("params") or {}
        session_id = str(payload.get("session_id") or "anonymous")
        if not isinstance(params, dict):
            self._send_json({"ok": False, "reason": "params must be an object"}, 400)
            return
        with self.server.lock:
            ok, reason = handle_action(self.server, action, params, session_id)
            self._send_json({"ok": ok, "reason": reason, "state": self.server.state.to_dict()})


def _as_float(value: Any, name: str) -> Tuple[Optional[float], Optional[str]]:
    try:
        number = float(value)
    except Exception:
        return None, f"{name} is not a number"
    if not math.isfinite(number):
        return None, f"{name} is not finite"
    return number, None


def _as_optional_int(value: Any, name: str) -> Tuple[Optional[int], Optional[str]]:
    if value is None or str(value).strip() == "":
        return None, None
    try:
        number = int(str(value).strip())
    except Exception:
        return None, f"{name} must be an integer"
    if number <= 0:
        return None, f"{name} must be positive"
    return number, None


def _parse_sexagesimal_angle(value: Any, name: str, *, is_ra: bool) -> Tuple[Optional[float], Optional[str]]:
    text = str(value or "").strip()
    if not text:
        return None, f"{name} is empty"
    try:
        number = float(text)
    except Exception:
        number = None
    if number is not None and math.isfinite(number):
        return number, None
    s = text.replace("−", "-").strip().lower()
    sign = 1.0
    if s.startswith("+"):
        s = s[1:].strip()
    if s.startswith("-"):
        sign = -1.0
        s = s[1:].strip()
    if is_ra and sign < 0:
        return None, "RA cannot be negative"
    for old in ["h", "d", "°"]:
        s = s.replace(old, ":")
    for old in ["m", "′", "'"]:
        s = s.replace(old, ":")
    for old in ["s", "″", '"']:
        s = s.replace(old, "")
    s = ":".join(part for part in s.replace("\t", " ").split(" ") if part)
    while "::" in s:
        s = s.replace("::", ":")
    s = s.strip(":")
    try:
        parts = [float(x) for x in s.split(":") if x != ""]
    except Exception:
        return None, f"{name} must be decimal degrees or sexagesimal"
    if len(parts) < 1 or len(parts) > 3 or not all(math.isfinite(x) for x in parts):
        return None, f"{name} must be decimal degrees or sexagesimal"
    a0 = abs(parts[0])
    a1 = parts[1] if len(parts) > 1 else 0.0
    a2 = parts[2] if len(parts) > 2 else 0.0
    if not (0 <= a1 < 60) or not (0 <= a2 < 60):
        return None, "sexagesimal minutes/seconds must be in 0..60"
    value_abs = a0 + a1 / 60.0 + a2 / 3600.0
    degrees = value_abs * 15.0 if is_ra else sign * value_abs
    return degrees, None


def _validate_mount(params: Dict[str, Any], limits: Optional[Dict[str, float]] = None) -> Tuple[bool, str, Optional[float], Optional[float]]:
    az, err = _as_float(params.get("az"), "Az")
    if err:
        return False, err, None, None
    el, err = _as_float(params.get("el"), "El")
    if err:
        return False, err, None, None
    limits = limits or (params.get("limits") if isinstance(params.get("limits"), dict) else {})
    az_min, err = _as_float(limits.get("az_min", 5), "Az min")
    if err:
        return False, err, None, None
    az_max, err = _as_float(limits.get("az_max", 355), "Az max")
    if err:
        return False, err, None, None
    el_min, err = _as_float(limits.get("el_min", 5), "El min")
    if err:
        return False, err, None, None
    el_max, err = _as_float(limits.get("el_max", 85), "El max")
    if err:
        return False, err, None, None
    assert az is not None and el is not None
    assert az_min is not None and az_max is not None
    assert el_min is not None and el_max is not None
    if az_min >= az_max:
        return False, "Az min/max are invalid", None, None
    if el_min >= el_max:
        return False, "El min/max are invalid", None, None
    if not (az_min <= az <= az_max):
        return False, f"Az={az:g} deg is outside allowed range {az_min:g}..{az_max:g} deg", None, None
    if not (el_min <= el <= el_max):
        return False, f"El={el:g} deg is outside allowed range {el_min:g}..{el_max:g} deg", None, None
    return True, "ok", az, el


def _validate_observation(params: Dict[str, Any]) -> Tuple[bool, str]:
    mode = str(params.get("mode") or "")
    path = str(params.get("file") or "").strip()
    if mode not in {"otf", "psw", "grid", "radio_pointing"}:
        return False, f"unsupported observation mode: {mode!r}"
    if not path:
        return False, "obs file path/name is empty"
    if not path.lower().endswith((".obs", ".toml")):
        return False, "obs file extension should be .obs or .toml"
    channel, err = _as_optional_int(params.get("channel"), "channel")
    if err:
        return False, err
    if channel is not None and channel > 30:
        return False, "channel override is too large for this demo"
    return True, "ok"


def _observation_check_summary(params: Dict[str, Any]) -> str:
    mode = str(params.get("mode") or "")
    path = str(params.get("file") or "").strip()
    channel, _err = _as_optional_int(params.get("channel"), "channel")
    preview = str(params.get("preview") or "")
    defaults = "defaults: cos_correction=true, use_scan_block=true, sg_preflight_policy=verify_only, sg_preflight_scope=active_lo_chains"
    ch_text = f", channel override={channel}" if channel is not None else ", channel=obs file/default"
    preview_text = f", preview={len(preview.splitlines()) if preview else 0} lines"
    return f"mode={mode}, file={path}{ch_text}{preview_text}, {defaults}"


def _observation_dry_run_summary(params: Dict[str, Any]) -> str:
    mode = str(params.get("mode") or "")
    path = str(params.get("file") or "").strip()
    preview = str(params.get("preview") or "")
    preview_lines = len(preview.splitlines()) if preview else 0
    if mode == "otf":
        plan = "plan=OTF sequence: parse obs -> apply defaults -> build scan blocks when available -> verify SG targets"
    elif mode == "psw":
        plan = "plan=PSW sequence: parse obs -> apply defaults -> build ON/OFF/HOT cadence -> verify SG targets"
    elif mode == "grid":
        plan = "plan=Grid sequence: parse obs -> apply defaults -> build grid point list -> verify SG targets"
    elif mode == "radio_pointing":
        plan = "plan=Radio Pointing sequence: parse obs -> apply defaults -> build pointing scans -> verify SG targets"
    else:
        plan = "plan=unknown"
    return f"Dry run OK: file={path}, preview={preview_lines} lines, {plan}. No mount/chopper/recording/gate/SG apply commands were sent."


def _validate_tracking(params: Dict[str, Any]) -> Tuple[bool, str]:
    kind = str(params.get("kind") or "")
    if kind not in {"sun", "moon", "name", "radec", "galactic"}:
        return False, f"unsupported target kind: {kind!r}"
    if kind == "name" and not str(params.get("name") or "").strip():
        return False, "object name is empty"
    if kind == "radec":
        ra_deg, err = _parse_sexagesimal_angle(params.get("coord1"), "RA", is_ra=True)
        if err:
            return False, err
        dec_deg, err = _parse_sexagesimal_angle(params.get("coord2"), "Dec", is_ra=False)
        if err:
            return False, err
        assert ra_deg is not None and dec_deg is not None
        if not (0.0 <= ra_deg < 360.0):
            return False, "RA must satisfy 0 <= value < 360 deg"
        if not (-90.0 <= dec_deg <= 90.0):
            return False, "Dec must satisfy -90 <= value <= 90 deg"
    if kind == "galactic":
        c1, err = _as_float(params.get("coord1"), "Galactic l")
        if err:
            return False, err
        c2, err = _as_float(params.get("coord2"), "Galactic b")
        if err:
            return False, err
        assert c1 is not None and c2 is not None
        if not (0.0 <= c1 < 360.0):
            return False, "Galactic l must satisfy 0 <= value < 360 deg"
        if not (-90.0 <= c2 <= 90.0):
            return False, "Galactic b must satisfy -90 <= value <= 90 deg"
    off_x, err = _as_float(params.get("offset_x_arcsec", 0), "offset X")
    if err:
        return False, err
    off_y, err = _as_float(params.get("offset_y_arcsec", 0), "offset Y")
    if err:
        return False, err
    assert off_x is not None and off_y is not None
    if abs(off_x) > 36000 or abs(off_y) > 36000:
        return False, "offset is larger than demo limit 36000 arcsec"
    return True, "ok"


def _validate_positive_float(value: Any, name: str) -> Tuple[bool, str, Optional[float]]:
    number, err = _as_float(value, name)
    if err:
        return False, err, None
    assert number is not None
    if number <= 0:
        return False, f"{name} must be positive", None
    return True, "ok", number


def _with_authority(server: ConsoleDemoServer, session_id: str, action_text: str) -> str:
    state = server.state
    if state.authority_held:
        if state.authority_session_id == session_id:
            return f"{action_text} using held authority"
        return f"{action_text} rejected: authority is held by another browser session"
    return f"{action_text} with temporary authority acquire/release"



EXCLUSIVE_START_ACTIONS = {
    "mount_move",
    "start_tracking",
    "start_observation",
    "run_rsky",
    "run_skydip",
}
EXCLUSIVE_START_BLOCK_SEC = 3.0
EXCLUSIVE_START_STATUS_SEC = 15.0


def _demo_clear_exclusive_start_guard(state: DemoState) -> None:
    state.exclusive_start_action = None
    state.exclusive_start_started_at = None
    state.exclusive_start_message = ""



def _demo_set_record(state: DemoState, record_name: str) -> None:
    record = str(record_name or '').strip() or 'demo_record'
    state.record_name = record
    state.recording_dir = f"/root/data/{record}"
    state.local_recording_dir = f"/local/necst/data/{record}"
    state.progress_record_dir = f"/tmp/necst_progress/{record}"

def _demo_mark_exclusive_start_guard(state: DemoState, action: str, message: str) -> None:
    if action not in EXCLUSIVE_START_ACTIONS:
        return
    state.exclusive_start_action = str(action)
    state.exclusive_start_started_at = time.time()
    state.exclusive_start_message = str(message or action)


def _demo_active_operation_reason(state: DemoState, requested_action: str) -> Optional[str]:
    """Reject non-safety start actions while another operation is active.

    Browser-side disabled buttons are helpful, but operators can double-click,
    keep stale tabs open, or hit the HTTP endpoint directly.  The demo server
    mirrors the real console guard so the UI never relies on frontend state as
    the only protection against conflicting starts.
    """

    if requested_action not in EXCLUSIVE_START_ACTIONS:
        return None
    guard_action = str(state.exclusive_start_action or "").strip()
    guard_started = state.exclusive_start_started_at
    if guard_action and guard_started is not None:
        age = max(0.0, time.time() - float(guard_started))
        if age <= EXCLUSIVE_START_BLOCK_SEC:
            message = state.exclusive_start_message or guard_action
            return f"cannot start {requested_action}: {message} was accepted {age:.1f} s ago; wait for READY or use STOP/ABORT"
        _demo_clear_exclusive_start_guard(state)
    sys_state = str(state.state or "").strip().lower()
    manual = str(state.manual_state or "").strip().lower()
    task = str(state.active_task or "").strip().lower()
    if sys_state in {"observing", "calibrating"}:
        return f"cannot start {requested_action}: system is {state.state}; use STOP or ABORT before starting another operation"
    if manual in {"moving", "tracking", "calibration", "observing sequence"}:
        return f"cannot start {requested_action}: current manual state is {state.manual_state}; use STOP or wait until READY"
    if task not in {"", "none", "idle", "unknown"}:
        return f"cannot start {requested_action}: active task is {state.active_task}; use STOP/ABORT or wait until READY"
    return None

def handle_action(
    server: ConsoleDemoServer, action: str, params: Dict[str, Any], session_id: str
) -> Tuple[bool, str]:
    state = server.state

    active_reason = _demo_active_operation_reason(state, action)
    if active_reason is not None:
        server.add_log(False, active_reason)
        return False, active_reason

    if action == "launch_progress":
        ok, reason, url = server.launch_progress()
        server.add_log(ok, f"{reason}: {url}")
        return ok, reason

    if action == "clear_log":
        state.log.clear()
        server.add_log(True, "operation log cleared")
        return True, "cleared"

    if action == "clear_stale_observation_state":
        _demo_clear_exclusive_start_guard(state)
        state.state = "idle"
        state.manual_state = "idle"
        state.active_task = "none"
        state.command_az = None
        state.command_el = None
        server.add_log(True, "stale observation state cleared in demo")
        return True, "stale observation state cleared"

    if action == "acquire_authority":
        if state.authority_held and state.authority_session_id != session_id:
            reason = "authority is already held by another browser session"
            server.add_log(False, reason)
            return False, reason
        state.authority_held = True
        state.authority_session_id = session_id
        server.add_log(True, "authority acquired")
        return True, "authority acquired"

    if action == "release_authority":
        if not state.authority_held:
            server.add_log(True, "authority was already free")
            return True, "authority already free"
        if state.authority_session_id != session_id:
            reason = "cannot release authority held by another browser session"
            server.add_log(False, reason)
            return False, reason
        state.authority_held = False
        state.authority_session_id = None
        server.add_log(True, "authority released")
        return True, "authority released"

    if action == "stop_tracking":
        if state.manual_state != "tracking":
            reason = "target tracking is not running"
            server.add_log(False, reason)
            return False, reason
        _demo_clear_exclusive_start_guard(state)
        state.command_az = None
        state.command_el = None
        state.manual_state = "stopped"
        state.active_task = "none"
        server.add_log(True, "Stop tracking sent: same antenna-stop operation as STOP, limited to tracking context")
        return True, "tracking stopped"

    if action == "stop":
        _demo_clear_exclusive_start_guard(state)
        state.command_az = None
        state.command_el = None
        state.manual_state = "stopped"
        if state.state == "observing":
            state.active_task = "observation; antenna STOP sent"
            server.add_log(True, "STOP sent: antenna/manual motion stopped; observation cleanup is not performed")
        elif state.state == "calibrating":
            state.active_task = "calibration; STOP sent"
            server.add_log(True, "STOP sent: motion stopped during calibration; procedure cleanup is not simulated")
        else:
            state.active_task = "none"
            server.add_log(True, "STOP sent: manual motion/tracking stopped")
        return True, "stopped"

    if action == "mount_move" or action == "mount_move_dry_run":
        ok, reason, az, el = _validate_mount(params, state.mount_limits)
        if not ok:
            server.add_log(False, f"mount move not sent: {reason}")
            return False, reason
        assert az is not None and el is not None
        prefix = "dry-run mount move" if action == "mount_move_dry_run" else "mount move"
        authority_msg = _with_authority(server, session_id, prefix)
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        if action == "mount_move":
            _demo_mark_exclusive_start_guard(state, action, f"mount move Az={az:.4f} deg El={el:.4f} deg")
            state.command_az = az
            state.command_el = el
            state.az = az
            state.el = el
            state.manual_state = "moving"
            state.active_task = "manual mount move"
        server.add_log(True, f"{authority_msg}: Az={az:.6f} deg, El={el:.6f} deg")
        return True, "ok"

    if action == "check_observation":
        ok, reason = _validate_observation(params)
        if ok:
            summary = _observation_check_summary(params)
            server.add_log(True, f"observation Check OK: {summary}")
            return True, "Check OK: obs file selection and static options are valid"
        server.add_log(False, f"observation Check failed: {reason}")
        return False, reason

    if action == "dry_run_observation":
        ok, reason = _validate_observation(params)
        if not ok:
            server.add_log(False, f"observation Dry run failed before planning: {reason}")
            return False, reason
        summary = _observation_dry_run_summary(params)
        server.add_log(True, summary)
        return True, summary

    if action == "start_observation":
        ok, reason = _validate_observation(params)
        if not ok:
            server.add_log(False, f"observation not started: {reason}")
            return False, reason
        authority_msg = _with_authority(server, session_id, "start observation")
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        _demo_mark_exclusive_start_guard(state, action, "observation")
        state.command_az = None
        state.command_el = None
        state.state = "observing"
        state.manual_state = "observing sequence"
        state.active_task = "observation"
        mode = str(params.get("mode") or "")
        path = str(params.get("file") or "").strip()
        stem = Path(path).name.rsplit('.', 1)[0] if path else 'demo_observation'
        _demo_set_record(state, f"{stem}_{int(time.time())}")
        server.add_log(True, f"{authority_msg}: mode={mode}, file={path}")
        return True, "observation started"

    if action == "abort_observation":
        if state.state != "observing":
            reason = "no running observation to abort"
            server.add_log(False, reason)
            return False, reason
        _demo_clear_exclusive_start_guard(state)
        state.command_az = None
        state.command_el = None
        state.state = "idle"
        state.manual_state = "idle"
        state.active_task = "none"
        server.add_log(True, "ABORT sent: observation cleanup requested")
        return True, "observation aborted"

    if action == "start_tracking":
        ok, reason = _validate_tracking(params)
        if not ok:
            server.add_log(False, f"tracking not started: {reason}")
            return False, reason
        authority_msg = _with_authority(server, session_id, "start tracking")
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        _demo_mark_exclusive_start_guard(state, action, "target tracking")
        state.command_az = None
        state.command_el = None
        state.manual_state = "tracking"
        state.active_task = "target tracking"
        kind = str(params.get("kind") or "")
        off_x = float(params.get("offset_x_arcsec") or 0)
        off_y = float(params.get("offset_y_arcsec") or 0)
        server.add_log(
            True,
            f"{authority_msg}: target={kind}, offset=({off_x:g}, {off_y:g}) arcsec; stop with STOP",
        )
        return True, "tracking started"

    if action.startswith("chopper_"):
        authority_msg = _with_authority(server, session_id, action.replace("_", " "))
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        if action == "chopper_in":
            state.chopper_state = "IN"
            state.chopper_position = 4750
        elif action == "chopper_out":
            state.chopper_state = "OUT"
            state.chopper_position = 19700
        elif action == "chopper_home":
            state.chopper_state = "HOME"
            state.chopper_position = 0
        elif action == "chopper_recover":
            state.chopper_state = "HOME"
            state.chopper_position = 0
        elif action == "chopper_alarm_reset":
            pass
        elif action == "chopper_status":
            server.add_log(True, f"chopper status: {state.chopper_state}, position={state.chopper_position}")
            return True, "status"
        else:
            return False, f"unknown chopper action: {action}"
        server.add_log(True, f"{authority_msg}: state={state.chopper_state}, position={state.chopper_position}")
        return True, "ok"

    if action == "run_rsky":
        n, err = _as_optional_int(params.get("n"), "RSky n")
        if err:
            server.add_log(False, f"RSky not started: {err}")
            return False, err
        ok, reason, integ = _validate_positive_float(params.get("integ", 5), "RSky integ")
        if not ok:
            server.add_log(False, f"RSky not started: {reason}")
            return False, reason
        _ch, err = _as_optional_int(params.get("ch"), "RSky channel")
        if err:
            server.add_log(False, f"RSky not started: {err}")
            return False, err
        authority_msg = _with_authority(server, session_id, "RSky")
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        _demo_mark_exclusive_start_guard(state, action, "RSky")
        state.command_az = None
        state.command_el = None
        state.state = "calibrating"
        state.manual_state = "calibration"
        state.active_task = "RSky"
        _demo_set_record(state, f"necst_rsky_{time.strftime('%Y%m%d_%H%M%S')}")
        server.add_log(True, f"{authority_msg}: n={n or 1}, integ={integ:g} s")
        return True, "RSky started"

    if action == "run_skydip":
        ok, reason, integ = _validate_positive_float(params.get("integ", 10), "SkyDip integ")
        if not ok:
            server.add_log(False, f"SkyDip not started: {reason}")
            return False, reason
        _ch, err = _as_optional_int(params.get("ch"), "SkyDip channel")
        if err:
            server.add_log(False, f"SkyDip not started: {err}")
            return False, err
        tp = str(params.get("tp_range") or "").split()
        if tp:
            try:
                tp_values = [int(x) for x in tp]
            except Exception:
                reason = "tp_range must be integers"
                server.add_log(False, f"SkyDip not started: {reason}")
                return False, reason
            if len(tp_values) % 2 != 0:
                reason = "tp_range must contain START END pairs"
                server.add_log(False, f"SkyDip not started: {reason}")
                return False, reason
        authority_msg = _with_authority(server, session_id, "SkyDip")
        if "rejected" in authority_msg:
            server.add_log(False, authority_msg)
            return False, authority_msg
        _demo_mark_exclusive_start_guard(state, action, "SkyDip")
        state.command_az = None
        state.command_el = None
        state.state = "calibrating"
        state.manual_state = "calibration"
        state.active_task = "SkyDip"
        _demo_set_record(state, f"necst_skydip_{time.strftime('%Y%m%d_%H%M%S')}")
        server.add_log(True, f"{authority_msg}: integ={integ:g} s")
        return True, "SkyDip started"

    reason = f"unknown action: {action!r}"
    server.add_log(False, reason)
    return False, reason


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview a standalone NECST operator-console GUI demo"
    )
    parser.add_argument("--host", default="127.0.0.1", help="web bind address")
    parser.add_argument("--port", type=int, default=8090, help="web bind port")
    parser.add_argument(
        "--telescope", default="OMU1.85m demo", help="telescope label shown in the UI"
    )
    parser.add_argument(
        "--progress-url",
        default="http://127.0.0.1:8091/",
        help="initial Progress monitor URL label before the managed demo progress server is launched",
    )
    parser.add_argument("--progress-host", default="127.0.0.1", help="bind address for the managed demo progress-monitor server")
    parser.add_argument("--progress-port", type=int, default=8091, help="port for the managed demo progress-monitor server")
    parser.add_argument("--open", action="store_true", help="open the URL in a browser")
    parser.add_argument("--quiet", action="store_true", help="suppress HTTP request logs")
    parser.add_argument("--az-min", type=float, default=5.0, help="demo Az lower limit, standing in for site TOML")
    parser.add_argument("--az-max", type=float, default=355.0, help="demo Az upper limit, standing in for site TOML")
    parser.add_argument("--el-min", type=float, default=5.0, help="demo El lower limit, standing in for site TOML")
    parser.add_argument("--el-max", type=float, default=85.0, help="demo El upper limit, standing in for site TOML")
    args = parser.parse_args(argv)

    server = ConsoleDemoServer(
        (str(args.host), int(args.port)),
        Handler,
        telescope=str(args.telescope),
        progress_url=str(args.progress_url),
        quiet=bool(args.quiet),
        progress_host=str(args.progress_host),
        progress_port=int(args.progress_port),
    )
    server.state.mount_limits = {
        "az_min": float(args.az_min),
        "az_max": float(args.az_max),
        "el_min": float(args.el_min),
        "el_max": float(args.el_max),
    }
    url = f"http://{args.host}:{args.port}/"
    print("NECST operator console demo v7")
    print("  standalone: no NECST/neclib/ROS import")
    print("  real telescope commands: disabled")
    print(f"  managed progress demo: http://{args.progress_host}:{args.progress_port}/ (launched from the UI)")
    print(f"  demo site limits: Az {args.az_min}..{args.az_max} deg, El {args.el_min}..{args.el_max} deg")
    print(f"  URL: {url}")
    if args.open:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    stop_event = threading.Event()

    def _signal_handler(_signum: int, _frame: Any) -> None:
        stop_event.set()
        server.shutdown()

    old_int = signal.signal(signal.SIGINT, _signal_handler)
    old_term = signal.signal(signal.SIGTERM, _signal_handler)
    thread = threading.Thread(target=server.serve_forever, name="console-demo", daemon=True)
    thread.start()
    try:
        while not stop_event.wait(0.2):
            pass
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)
        server.stop_progress()
        server.server_close()
        thread.join(timeout=2.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
