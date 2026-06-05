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
            <button id="startObs" class="primary large" disabled title="Start the selected observation sequence. Check and Dry run are optional; the server still validates the obs-file path and options before starting.">Start observation</button>
            <button id="abortObs" class="danger large" disabled title="Abort a running observation sequence and request recorder/gate/progress cleanup.">ABORT observation</button>
          </div>
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
        <div class="row">
          <div class="field">
            <label for="obsMode">Observation mode</label>
            <select id="obsMode" title="Observation modes supported by the obs-file launcher. RSky and SkyDip are calibration actions, not observation modes.">
              <option value="otf">OTF</option>
              <option value="psw">PSW</option>
              <option value="grid">Grid</option>
              <option value="radio_pointing">Radio Pointing</option>
            </select>
          </div>
          <div class="field">
            <label for="obsFile" title="Choose a file from this browser computer only to preview its text. The real NECST-side run path is computed below from directory + filename.">Choose local obs file for preview</label>
            <input id="obsFile" type="file" accept=".obs,.toml,.txt" title="Preview only. The browser does not expose the real local absolute path.">
            <small>This reads the local file into the preview box and copies its filename. Browsers do not provide the real local path.</small>
          </div>
        </div>
        <div class="row">
          <div class="field">
            <label for="recentDir" title="Directory on the NECST computer. The real implementation should remember recent directories.">Obs directory on NECST computer</label>
            <select id="recentDir" title="NECST-side directory used to construct the computed run path.">
              <option>/home/necst/obs</option>
              <option>/home/necst/obs/commissioning</option>
              <option>/data/observations/current</option>
            </select>
            <small>The real console should remember recently used directories.</small>
          </div>
          <div class="field">
            <label for="obsFilename" title="Filename on the NECST computer. Choosing a local preview file copies only its filename here.">Obs filename</label>
            <input id="obsFilename" placeholder="e.g. orion_otf.obs" title="Filename to combine with the NECST-side directory. Accepts .obs or .toml in this demo.">
            <small>Filled from the local preview file name, then editable.</small>
          </div>
        </div>
        <div class="field">
          <label for="obsPath" title="Read-only confirmation. This is computed from Obs directory + Obs filename; edit those fields if this path is wrong.">Computed run path on NECST computer</label>
          <input id="obsPath" readonly placeholder="directory + filename" title="Read-only computed path used by Start observation.">
          <small>Read-only confirmation. Start uses this computed NECST-side path. The preview text itself is not executed.</small>
        </div>
        <div id="obsValidation" class="notice">Set the NECST-side directory and filename.</div>
        <details open>
          <summary>Preview selected obs file</summary>
          <div class="field" style="margin-top:10px">
            <label for="obsPreview">Preview text</label>
            <textarea id="obsPreview" spellcheck="false" placeholder="Choose a local file above, or paste obs-file text here for visual checking." title="Preview/check copy only. The Start button uses the computed NECST-side run path, not this text."></textarea>
            <small id="previewInfo">Preview: empty</small>
          </div>
        </details>
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
          <div class="kv"><span>Command Az / El</span><b id="cmdSummary">none</b></div>
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
            <button id="chopperIn" class="primary">Chopper IN</button>
            <button id="chopperOut" class="secondary">Chopper OUT</button>
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
            <div class="card" style="box-shadow:none">
              <div class="card-head"><h2>RSky</h2><span class="hint">runtime parameters</span></div>
              <div class="card-body">
                <div class="row">
                  <div class="field"><label for="rskyN">n</label><input id="rskyN" value="1" inputmode="numeric"></div>
                  <div class="field"><label for="rskyInteg">integ [s]</label><input id="rskyInteg" value="2" inputmode="decimal"></div>
                </div>
                <details><summary>Advanced</summary><div class="field" style="margin-top:10px"><label for="rskyCh">channel</label><input id="rskyCh" placeholder="empty = default"></div></details>
                <button id="runRsky" class="primary">Run RSky</button>
              </div>
            </div>
            <div class="card" style="box-shadow:none">
              <div class="card-head"><h2>SkyDip</h2><span class="hint">runtime parameters</span></div>
              <div class="card-body">
                <div class="field"><label for="skydipInteg">integ [s]</label><input id="skydipInteg" value="2" inputmode="decimal"></div>
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
            <div class="path-text" title="Expected NECST recorder output directory">Data directory: <b id="runtimeRecordingDir">loading</b></div>
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
  lastSelfCheck: null
};
localStorage.setItem('necstConsoleDemoSession', state.sessionId);

function numberOrNaN(value) {
  if (String(value).trim() === '') return NaN;
  return Number(value);
}
function finite(value) { return Number.isFinite(value); }

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
function formatMaybeNumber(value, digits=3) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : String(value ?? 'unknown');
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
  setText('runtimeLiveTelemetry', liveTelemetry.requested === false ? 'disabled' : (liveTelemetry.available ? `available (${liveTelemetry.spin_mode || 'spin'})` : `unavailable${liveTelemetry.error ? ': ' + liveTelemetry.error : ''}`));
  setText('runtimeObservatory', site.observatory || 'unknown');
  setText('runtimeConfigSource', site.source || 'unknown');
  setText('runtimeConfigPath', site.source_path || '(none)');
  setText('runtimeCapabilities', capabilityText(data.capabilities || {}));
  setText('runtimeProcessCounts', processSummary(counts));
  const processEl = qs('runtimeProcesses');
  if (processEl) processEl.innerHTML = renderProcessRows(processes);
  const obs = data.observation || {};
  setText('runtimeRecordName', obs.record_name || '(none)');
  setText('runtimeRecordingDir', obs.recording_dir || '(not available yet)');
  setText('runtimeProgressRecordDir', obs.progress_record_dir || '(not available yet)');
  renderLauncherLogChoices(processes);
  setText('runtimeProgressMonitor', progressMonitorText(progress));
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
async function api(action, params={}) {
  const resp = await fetch('/api/action', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({action, params, session_id: state.sessionId})
  });
  const data = await resp.json();
  await refresh();
  return data;
}
async function refresh() {
  const resp = await fetch('/api/status');
  const data = await resp.json();
  renderStatus(data);
  renderLog(data.log || []);
}
function renderStatus(data) {
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
  const motionClass = (data.manual_state === 'moving' || data.manual_state === 'tracking' || data.manual_state === 'calibration' || data.manual_state === 'observing sequence') ? 'ok' : (data.manual_state === 'stopped' ? 'warn' : 'info');
  const cmd = (finite(Number(data.command_az)) && finite(Number(data.command_el))) ? `${Number(data.command_az).toFixed(3)} / ${Number(data.command_el).toFixed(3)}` : 'none';
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
  qs('obsStateBadge').textContent = data.state;
  qs('obsStateBadge').className = 'badge ' + (data.state === 'observing' ? 'ok' : (data.state === 'calibrating' ? 'warn' : 'info'));
  qs('abortObs').disabled = data.state !== 'observing';
  qs('manualBadge').textContent = data.manual_state;
  qs('manualBadge').className = 'badge ' + motionClass;
  qs('motionSummary').textContent = data.manual_state;
  qs('taskSummary').textContent = data.active_task || 'none';
  qs('posSummary').textContent = `${formatMaybeNumber(data.az)} / ${formatMaybeNumber(data.el)}`;
  qs('cmdSummary').textContent = cmd;
  qs('chopperSummary').textContent = `${data.chopper.state} / pos ${data.chopper.position}`;
  qs('chopperState').textContent = data.chopper.state;
  qs('chopperPos').textContent = data.chopper.position;
  qs('chopperAge').textContent = data.chopper.age || 'live demo';
  qs('stopTracking').disabled = data.manual_state !== 'tracking';
  qs('calStatusNotice').textContent = `Current active task: ${data.active_task || 'none'}`;
  qs('calStatusNotice').className = 'notice ' + (data.state === 'calibrating' ? 'ok' : '');
  renderRuntime(data);
  applyCapabilities(data);
}
function renderLog(items) {
  qs('log').innerHTML = items.map(item => {
    const statusClass = item.ok ? 'ok' : 'bad';
    return `<div class="log-entry"><span class="time">${item.time}</span> <span class="${statusClass}">${item.ok ? 'OK' : 'NG'}</span> ${item.message}</div>`;
  }).join('') || '<span style="color:var(--faint)">No operation yet.</span>';
}
function updateObsRunPath() {
  const dir = qs('recentDir').value.replace(/\/+$/, '');
  const name = qs('obsFilename').value.trim().replace(/^\/+/, '');
  qs('obsPath').value = (dir && name) ? `${dir}/${name}` : '';
  qs('runBarTitle').textContent = qs('obsPath').value || 'No obs file selected';
  qs('runBarSub').textContent = qs('obsPath').value ? `${qs('obsMode').value.toUpperCase()} / ${state.obsChecked ? 'checked' : 'unchecked, Start will validate'}` : 'Set the NECST-side directory and filename.';
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
['obsFilename','obsPreview','obsChannel','obsMode'].forEach(id => qs(id).addEventListener('input', () => { state.obsChecked = false; validateObs(); }));
['mountAz','mountEl'].forEach(id => qs(id).addEventListener('input', validateMount));
['targetKind','targetName','coord1','coord2','offsetFrame','offsetX','offsetY','cosCorrection'].forEach(id => qs(id).addEventListener('input', updateTargetFields));
qs('obsFile').addEventListener('change', async (ev) => {
  const file = ev.target.files[0];
  if (!file) return;
  qs('obsFilename').value = file.name;
  qs('obsPreview').value = await file.text();
  state.obsChecked = false;
  validateObs();
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
  await api('start_observation', {mode: qs('obsMode').value, file: qs('obsPath').value, channel: qs('obsChannel').value});
});
qs('abortObs').addEventListener('click', async () => {
  if (confirm('Abort current observation and request recorder/gate/progress cleanup?')) await api('abort_observation');
});
qs('authorityButton').addEventListener('click', async () => {
  const resp = await fetch('/api/status');
  const data = await resp.json();
  const heldByMe = data.authority && data.authority.session_id === state.sessionId;
  await api(heldByMe ? 'release_authority' : 'acquire_authority');
});
qs('stopButton').addEventListener('click', async () => { await api('stop'); });
qs('moveMount').addEventListener('click', async () => {
  const v = validateMount();
  if (!v.ok) return;
  await api('mount_move', {az: qs('mountAz').value, el: qs('mountEl').value});
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
  });
});
qs('stopTracking').addEventListener('click', async () => { await api('stop_tracking'); });
qs('chopperIn').addEventListener('click', () => api('chopper_in'));
qs('chopperOut').addEventListener('click', () => api('chopper_out'));
qs('chopperStatus').addEventListener('click', () => api('chopper_status'));
qs('chopperAlarmReset').addEventListener('click', () => api('chopper_alarm_reset'));
qs('chopperHome').addEventListener('click', () => api('chopper_home'));
qs('chopperRecover').addEventListener('click', () => api('chopper_recover'));
qs('runRsky').addEventListener('click', () => api('run_rsky', {n: qs('rskyN').value, integ: qs('rskyInteg').value, ch: qs('rskyCh').value}));
qs('runSkydip').addEventListener('click', () => api('run_skydip', {integ: qs('skydipInteg').value, ch: qs('skydipCh').value, tp_range: qs('skydipTpRange').value}));
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
qs('openProgress').addEventListener('click', async () => {
  const data = await api('launch_progress');
  const url = data.progress_url || (data.state && data.state.progress_url);
  if (data.ok && url) window.open(url, '_blank');
});
function getStatusRefreshMs() {
  const cfg = window.NECST_CONSOLE_CONFIG || {};
  const raw = Number(cfg.statusRefreshMs ?? cfg.status_refresh_ms ?? 1000);
  if (!Number.isFinite(raw)) return 1000;
  return Math.max(200, Math.round(raw));
}
const statusRefreshMs = getStatusRefreshMs();
validateObs(); updatePreviewInfo(); validateMount(); updateTargetFields(); refresh(); setInterval(refresh, statusRefreshMs);
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
    log: List[LogEntry] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "telescope": self.telescope,
            "progress_url": self.progress_url,
            "progress": {
                "url": self.progress_url,
                "running": self.progress_running,
                "owned_by_console": self.progress_owned_by_console,
            },
            "state": self.state,
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


def _with_privilege(server: ConsoleDemoServer, session_id: str, action_text: str) -> str:
    state = server.state
    if state.authority_held:
        if state.authority_session_id == session_id:
            return f"{action_text} using held authority"
        return f"{action_text} rejected: authority is held by another browser session"
    return f"{action_text} with temporary authority acquire/release"


def handle_action(
    server: ConsoleDemoServer, action: str, params: Dict[str, Any], session_id: str
) -> Tuple[bool, str]:
    state = server.state

    if action == "launch_progress":
        ok, reason, url = server.launch_progress()
        server.add_log(ok, f"{reason}: {url}")
        return ok, reason

    if action == "clear_log":
        state.log.clear()
        server.add_log(True, "operation log cleared")
        return True, "cleared"

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
        state.command_az = None
        state.command_el = None
        state.manual_state = "stopped"
        state.active_task = "none"
        server.add_log(True, "Stop tracking sent: same antenna-stop operation as STOP, limited to tracking context")
        return True, "tracking stopped"

    if action == "stop":
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
        privilege_msg = _with_privilege(server, session_id, prefix)
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
        if action == "mount_move":
            state.command_az = az
            state.command_el = el
            state.az = az
            state.el = el
            state.manual_state = "moving"
            state.active_task = "manual mount move"
        server.add_log(True, f"{privilege_msg}: Az={az:.6f} deg, El={el:.6f} deg")
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
        privilege_msg = _with_privilege(server, session_id, "start observation")
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
        state.command_az = None
        state.command_el = None
        state.state = "observing"
        state.manual_state = "observing sequence"
        state.active_task = "observation"
        mode = str(params.get("mode") or "")
        path = str(params.get("file") or "").strip()
        server.add_log(True, f"{privilege_msg}: mode={mode}, file={path}")
        return True, "observation started"

    if action == "abort_observation":
        if state.state != "observing":
            reason = "no running observation to abort"
            server.add_log(False, reason)
            return False, reason
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
        privilege_msg = _with_privilege(server, session_id, "start tracking")
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
        state.command_az = None
        state.command_el = None
        state.manual_state = "tracking"
        state.active_task = "target tracking"
        kind = str(params.get("kind") or "")
        off_x = float(params.get("offset_x_arcsec") or 0)
        off_y = float(params.get("offset_y_arcsec") or 0)
        server.add_log(
            True,
            f"{privilege_msg}: target={kind}, offset=({off_x:g}, {off_y:g}) arcsec; stop with STOP",
        )
        return True, "tracking started"

    if action.startswith("chopper_"):
        privilege_msg = _with_privilege(server, session_id, action.replace("_", " "))
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
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
        server.add_log(True, f"{privilege_msg}: state={state.chopper_state}, position={state.chopper_position}")
        return True, "ok"

    if action == "run_rsky":
        n, err = _as_optional_int(params.get("n"), "RSky n")
        if err:
            server.add_log(False, f"RSky not started: {err}")
            return False, err
        ok, reason, integ = _validate_positive_float(params.get("integ", 2), "RSky integ")
        if not ok:
            server.add_log(False, f"RSky not started: {reason}")
            return False, reason
        _ch, err = _as_optional_int(params.get("ch"), "RSky channel")
        if err:
            server.add_log(False, f"RSky not started: {err}")
            return False, err
        privilege_msg = _with_privilege(server, session_id, "RSky")
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
        state.command_az = None
        state.command_el = None
        state.state = "calibrating"
        state.manual_state = "calibration"
        state.active_task = "RSky"
        server.add_log(True, f"{privilege_msg}: n={n or 1}, integ={integ:g} s")
        return True, "RSky started"

    if action == "run_skydip":
        ok, reason, integ = _validate_positive_float(params.get("integ", 2), "SkyDip integ")
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
        privilege_msg = _with_privilege(server, session_id, "SkyDip")
        if "rejected" in privilege_msg:
            server.add_log(False, privilege_msg)
            return False, privilege_msg
        state.command_az = None
        state.command_el = None
        state.state = "calibrating"
        state.manual_state = "calibration"
        state.active_task = "SkyDip"
        server.add_log(True, f"{privilege_msg}: integ={integ:g} s")
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
