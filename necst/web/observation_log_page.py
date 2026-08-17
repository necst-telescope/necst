"""Standalone Observation Log detail page shared by the real console and demo."""

from __future__ import annotations


def render_observation_log_page() -> str:
    """Return the browser UI for ``/observation-log``.

    The page deliberately talks to the existing console APIs.  That keeps the
    detailed view usable from both the ROS-backed console and the standalone
    demo without creating a second CSV writer or a second comment protocol.
    """

    return r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Observation Log</title>
<style>
:root {
  --bg:#090f20; --panel:#111a31; --panel2:#17233f; --line:#2b3858;
  --text:#eaf0ff; --muted:#aab7d6; --accent:#79a5ff; --ok:#57d364;
  --bad:#ff7373; --selection-soft:#213f78; --selection-strong:#3968ba;
  --selection-active:#5b8ff0; --selection-ring:#a8c7ff;
}
* { box-sizing:border-box; }
body { margin:0; min-height:100vh; color:var(--text); background:radial-gradient(circle at top left,#1d315f 0,var(--bg) 34rem); font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
button,input,select { font:inherit; }
button { border:1px solid var(--line); border-radius:9px; color:var(--text); background:#1a2746; padding:8px 12px; cursor:pointer; }
button:hover:not(:disabled) { border-color:var(--accent); background:#22345e; }
button:disabled { opacity:.45; cursor:not-allowed; }
button.primary { background:#2451a6; border-color:#6e9bff; }
input,select { min-width:0; width:100%; color:var(--text); background:#0d152a; border:1px solid var(--line); border-radius:9px; padding:9px 10px; outline:none; }
input:focus,select:focus { border-color:var(--accent); }
.top { position:sticky; top:0; z-index:10; border-bottom:1px solid var(--line); background:rgba(9,15,32,.95); backdrop-filter:blur(10px); }
.top-inner { max-width:2200px; margin:auto; padding:14px 18px 12px; }
.crumb { color:var(--muted); font-size:12px; }
h1 { margin:2px 0 10px; font-size:24px; }
.controls { display:grid; grid-template-columns:minmax(220px,1.5fr) minmax(150px,.8fr) minmax(150px,.8fr) auto auto auto; gap:8px; align-items:center; }
.editor { display:grid; grid-template-columns:minmax(240px,1fr) auto auto; gap:8px; align-items:center; margin-top:10px; }
.target { color:var(--muted); white-space:nowrap; }
.message { min-height:20px; color:var(--muted); margin-top:8px; }
.message.bad { color:var(--bad); }
.message.ok { color:var(--ok); }
.main { max-width:2200px; margin:auto; padding:16px 18px 24px; }
.table-box { overflow:auto; max-height:calc(100vh - 205px); border:1px solid var(--line); border-radius:12px; background:rgba(7,12,26,.72); }
table { border-collapse:separate; border-spacing:0; min-width:max-content; width:100%; table-layout:auto; }
th,td { border-right:1px solid #25314d; border-bottom:1px solid #25314d; padding:8px 10px; height:40px; max-width:520px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; text-align:left; }
th { position:sticky; top:0; z-index:3; color:#b8c5e2; background:#16213a; font-weight:700; cursor:pointer; }
th:first-child { left:0; z-index:5; }
th[data-col="__row"],td[data-row-select] { min-width:58px; width:58px; text-align:center; user-select:none; }
td:first-child { position:sticky; left:0; z-index:2; background:#111a31; color:#b8c5e2; font-variant-numeric:tabular-nums; cursor:pointer; }
th[data-col="__row"] { cursor:pointer; }
th[data-col="comment"],td[data-col="comment"] { width:220px; min-width:220px; max-width:220px; }
tr.selected td { background:var(--selection-soft); }
tr.selected td:first-child { background:var(--selection-strong); color:#fff; }
th.selected { background:var(--selection-strong); color:#fff; border-right-color:var(--selection-active);
  box-shadow:inset 0 3px 0 var(--selection-ring),inset 0 -3px 0 var(--selection-active);
  text-shadow:0 1px 2px rgba(0,0,0,.35); }
th[data-col]:not([data-col="__row"]).selected { background:var(--selection-strong); }
td.selected-column { background:var(--selection-soft); }
tr.selected td.selected-column { background:var(--selection-soft); }
td.selected-cell { background:var(--selection-soft); box-shadow:inset 0 0 0 2px var(--selection-ring); }
td.editable { cursor:text; }
td.editable:hover { background:#1b2d52; }
td input.cell-editor { min-width:0; max-width:100%; width:100%; padding:4px 6px; border-radius:5px; }
.empty { padding:30px; color:var(--muted); }
.help { color:var(--muted); margin:10px 0 0; }
@media (max-width:900px) {
  .controls { grid-template-columns:1fr 1fr; }
  .editor { grid-template-columns:1fr auto; }
  .target { grid-column:1/-1; }
  .table-box { max-height:calc(100vh - 285px); }
}
</style>
</head>
<body>
<header class="top"><div class="top-inner">
  <div class="crumb">NECST / Operator Console / 8092 / observation-log</div>
  <h1>Observation Log</h1>
  <div class="controls">
    <select id="csv" title="CSV file"></select>
    <select id="order" title="UTC order"><option value="newest">UTC: newest first</option><option value="oldest">UTC: oldest first</option></select>
    <select id="event" title="Event filter"><option value="all">Event: all</option></select>
    <button id="newCsv" type="button">New CSV</button>
    <button id="refresh" type="button">Refresh</button>
    <button id="copy" type="button" class="primary">Copy selection</button>
  </div>
  <div class="editor">
    <input id="comment" placeholder="Standalone comment or selected row comment" title="Comment for the selected row">
    <button id="save" type="button" class="primary">Save</button>
    <div id="target" class="target">Target row: none</div>
  </div>
  <div id="message" class="message"></div>
</div></header>
<main class="main">
  <div class="help">Click a row-number or column header to select it. Shift selects a range; Cmd/Ctrl adds rows or columns.
    Copy selection copies the selected row × column matrix. Click a comment cell to edit it directly.</div>
  <div class="table-box"><table id="grid"><thead></thead><tbody></tbody></table><div id="empty" class="empty" hidden>No observation-log rows.</div></div>
</main>
<script>
(() => {
  const state = {rows:[], files:[], columns:[], order:'newest', event:'all', selectedRows:new Set(), selectedCols:new Set(), selectedCells:new Set(), rowAnchor:null, colAnchor:null, targetRow:null};
  const q = id => document.getElementById(id);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const text = value => String(value ?? '');
  const rowId = row => text(row && row.row_id);
  const cellKey = (rid,col) => rid + '::' + col;
  const rowById = rid => state.rows.find(row => rowId(row) === String(rid));
  const visibleRows = () => {
    const rows = state.rows.filter(row => state.event === 'all' || text(row.event) === state.event);
    rows.sort((a,b) => text(a.utc_iso).localeCompare(text(b.utc_iso)) * (state.order === 'oldest' ? 1 : -1));
    return rows;
  };
  const show = (value, max=120) => { const s=text(value); return s.length > max ? s.slice(0,max-1) + '…' : s; };
  function setMessage(value, kind='') { q('message').textContent = value || ''; q('message').className = 'message ' + kind; }
  async function getJson(url) {
    const response=await fetch(url,{cache:'no-store'}); const data=await response.json();
    if(!response.ok || data.ok===false) throw new Error(data.reason || ('HTTP '+response.status));
    return data;
  }
  async function action(action, params={}) {
    const response = await fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,params,session_id:'observation-log-detail'})});
    const data = await response.json(); if(!response.ok || !data.ok) throw new Error(data.reason || ('HTTP '+response.status)); return data;
  }
  function buildColumns(rows) {
    const preferred=['utc_iso','enc_az_deg','enc_el_deg','comment','mode','event',
      'action_or_obsfile','result','user','target','record_dir','session_id',
      'target_row_id','temp_C','humidity_pct','pressure_hPa','weather_source'];
    const present=new Set(rows.flatMap(row => Object.keys(row || {})));
    return preferred.filter(col => col === 'row_id' || present.has(col));
  }
  function updateFilters() {
    const events=[...new Set(state.rows.map(row=>text(row.event)).filter(Boolean))].sort();
    const current=state.event;
    q('event').innerHTML='<option value="all">Event: all</option>'+
      events.map(value=>`<option value="${esc(value)}">${esc(value)}</option>`).join('');
    q('event').value=events.includes(current)?current:'all'; state.event=q('event').value;
  }
  function updateFiles() {
    const current=state.files.find(file=>file.active);
    q('csv').innerHTML=state.files.map(file=>`<option value="${esc(file.path)}" ${file.active?'selected':''}>${esc(file.name)}${file.active?' · current':''}</option>`).join('');
    if (!state.files.length) q('csv').innerHTML='<option value="">No CSV files</option>'; q('csv').title=current ? current.path : '';
  }
  function selectionForCopy(rows) {
    const selectedRows=state.selectedRows.size ? rows.filter(row=>state.selectedRows.has(rowId(row))) : rows;
    const selectedCols=state.selectedCols.size ? state.columns.filter(col=>state.selectedCols.has(col)) : state.columns;
    if (state.selectedCells.size && !state.selectedRows.size && !state.selectedCols.size) {
      const cells=[]; rows.forEach(row => state.columns.forEach(col => { if(state.selectedCells.has(cellKey(rowId(row),col))) cells.push([row,col]); }));
      if (cells.length) return {rows:[...new Map(cells.map(([row])=>[rowId(row),row])).values()],cols:[...new Set(cells.map(([,col])=>col))]};
    }
    return {rows:selectedRows,cols:selectedCols};
  }
  function markdown(value) { return text(value).replace(/\|/g,'\\|').replace(/\r?\n/g,'<br>'); }
  function copySelection() {
    const rows=visibleRows(); const picked=selectionForCopy(rows); if(!picked.rows.length || !picked.cols.length) return setMessage('Nothing selected','bad');
    const lines=['| '+picked.cols.map(markdown).join(' | ')+' |',
      '| '+picked.cols.map(()=> '---').join(' | ')+' |'];
    picked.rows.forEach(row=>lines.push(
      '| '+picked.cols.map(col=>markdown(row[col])).join(' | ')+' |'));
    navigator.clipboard.writeText(lines.join('\n')).then(()=>setMessage('Copied Markdown table','ok')).catch(err=>setMessage('Copy failed: '+err,'bad'));
  }
  function syncEditor(rid) {
    state.targetRow=rid ? String(rid) : null; const row=rid ? rowById(rid) : null;
    q('target').textContent='Target row: '+(rid || 'none'); q('comment').value=row ? text(row.comment) : '';
  }
  function selectRow(rid, index, event) {
    const rows=visibleRows(); const ids=rows.map(rowId); const anchor=state.rowAnchor === null ? -1 : ids.indexOf(String(state.rowAnchor));
    if(event.shiftKey && anchor >= 0) {
      state.selectedRows=new Set(ids.slice(Math.min(anchor,index),Math.max(anchor,index)+1));
    } else if(event.metaKey || event.ctrlKey) {
      const next=new Set(state.selectedRows); next.has(String(rid))?next.delete(String(rid)):next.add(String(rid)); state.selectedRows=next; state.rowAnchor=String(rid);
    } else {
      state.selectedRows=new Set([String(rid)]); state.rowAnchor=String(rid);
    }
    state.selectedCells.clear(); syncEditor(rid); render();
  }
  function selectColumn(col,index,event) {
    const anchor=state.colAnchor === null ? -1 : state.columns.indexOf(state.colAnchor);
    if(event.shiftKey && anchor >= 0) {
      state.selectedCols=new Set(state.columns.slice(Math.min(anchor,index),Math.max(anchor,index)+1));
    } else if(event.metaKey || event.ctrlKey) {
      const next=new Set(state.selectedCols); next.has(col)?next.delete(col):next.add(col); state.selectedCols=next; state.colAnchor=col;
    } else {
      state.selectedCols=new Set([col]); state.colAnchor=col;
    }
    state.selectedCells.clear(); render();
  }
  function selectCell(rid,col,event) {
    if(event.metaKey || event.ctrlKey) {
      const key=cellKey(rid,col);
      state.selectedCells.has(key)?state.selectedCells.delete(key):state.selectedCells.add(key);
    } else {
      state.selectedCells=new Set([cellKey(rid,col)]); state.selectedRows.clear(); state.selectedCols.clear();
    }
    syncEditor(rid); render();
  }
  function clearSelection() {
    state.selectedRows.clear(); state.selectedCols.clear(); state.selectedCells.clear();
    state.rowAnchor=null; state.colAnchor=null; state.targetRow=null;
    syncEditor(null); render();
  }
  function commitCell(cell,input,original) {
    const value=input.value; cell.textContent=show(value); cell.classList.remove('editing');
    if(value===original) return;
    action('obslog_comment',{target_row_id:cell.dataset.rid,comment:value}).then(()=>load()).catch(err=>{
      cell.textContent=show(original);setMessage(String(err),'bad');
    });
  }
  function editComment(cell,row) {
    if(cell.querySelector('input')) return;
    const original=text(row.comment); const input=document.createElement('input');
    input.className='cell-editor'; input.value=original; cell.textContent=''; cell.appendChild(input);
    input.focus(); input.select();
    const finish=()=>{ if(input.dataset.done) return; input.dataset.done='1'; commitCell(cell,input,original); };
    input.addEventListener('blur',finish);
    input.addEventListener('keydown',event=>{
      if(event.key==='Escape'){input.dataset.done='1';cell.textContent=show(original);}
      else if(event.key==='Enter' || (event.key==='Enter' && (event.metaKey||event.ctrlKey))){event.preventDefault();finish();}
    });
  }
  function render() {
    const rows=visibleRows(); const head=q('grid').querySelector('thead'); const body=q('grid').querySelector('tbody');
    const allRowsSelected=rows.length > 0 && rows.every(row => state.selectedRows.has(rowId(row)));
    head.innerHTML='<tr><th data-col="__row" class="'+(allRowsSelected?'selected':'')+'" title="Select all visible rows">#</th>'+
      state.columns.map((col,index)=>`<th data-col="${esc(col)}" class="${state.selectedCols.has(col)?'selected':''}" title="Click to select column">${esc(col)}</th>`).join('')+'</tr>';
    body.innerHTML=rows.map((row,index)=>{
      const rid=rowId(row); const selected=state.selectedRows.has(rid);
      const cells=state.columns.map(col=>{
        const key=cellKey(rid,col); const value=show(row[col]); const editing=col==='comment';
        const selectedColumn=state.selectedCols.has(col);
        const classes=(editing?'editable ':'')+(selectedColumn?'selected-column ':'')+
          (state.selectedCells.has(key)?'selected-cell':'');
        return `<td data-rid="${esc(rid)}" data-col="${esc(col)}" `+
          `class="${classes}" title="${esc(text(row[col]))}">${esc(value)}</td>`;
      }).join('');
      return `<tr class="${selected?'selected':''}"><td data-row-select="${esc(rid)}" data-index="${index}" title="Select row">${esc(rid)}</td>${cells}</tr>`;
    }).join('');
    q('empty').hidden=rows.length!==0; q('grid').hidden=rows.length===0;
    head.querySelectorAll('th[data-col]').forEach(th=>th.addEventListener('click',event=>{
      const col=th.dataset.col;
      if(col==='__row') {
        const ids=rows.map(rowId); const anchor=state.rowAnchor === null ? -1 : ids.indexOf(String(state.rowAnchor));
        if(event.shiftKey && anchor >= 0) {
          state.selectedRows=new Set(ids.slice(Math.min(anchor,ids.length-1),Math.max(anchor,ids.length-1)+1));
        } else if(event.metaKey || event.ctrlKey) {
          const allSelected=ids.length > 0 && ids.every(rid => state.selectedRows.has(rid));
          const next=new Set(state.selectedRows); ids.forEach(rid=>allSelected?next.delete(rid):next.add(rid)); state.selectedRows=next;
        } else {
          state.selectedRows=new Set(ids);
        }
        state.rowAnchor=ids.length ? ids[ids.length-1] : null; state.selectedCells.clear(); render();
      } else selectColumn(col,state.columns.indexOf(col),event);
    }));
    body.querySelectorAll('td[data-row-select]').forEach(td=>td.addEventListener('click',event=>selectRow(td.dataset.rowSelect,Number(td.dataset.index),event)));
    body.querySelectorAll('td[data-col]').forEach(td=>td.addEventListener('click',event=>{
      const row=rowById(td.dataset.rid); if(!row)return;
      const rid=td.dataset.rid; const col=td.dataset.col; selectCell(rid,col,event);
      if(col==='comment'){
        const fresh=[...body.querySelectorAll('td[data-col]')].find(cell=>cell.dataset.rid===rid && cell.dataset.col===col);
        if(fresh) editComment(fresh,row);
      }
    }));
  }
  async function load() {
    try {
      const [rows,files]=await Promise.all([
        getJson('/api/observation-log-rows?limit=5000'),
        getJson('/api/observation-log-files?limit=500')
      ]);
      state.rows=Array.isArray(rows.rows)?rows.rows:[]; state.files=Array.isArray(files.files)?files.files:[];
      state.columns=buildColumns(state.rows); updateFilters(); updateFiles(); render();
      if(state.targetRow) syncEditor(state.targetRow); setMessage(`${state.rows.length} row(s)`);
    } catch(err) { setMessage(String(err),'bad'); }
  }
  q('order').addEventListener('change',event=>{state.order=event.target.value;render();});
  q('event').addEventListener('change',event=>{state.event=event.target.value;render();});
  q('refresh').addEventListener('click',load); q('copy').addEventListener('click',copySelection);
  q('comment').addEventListener('input',()=>{});
  q('save').addEventListener('click',async()=>{
    try {
      await action('obslog_comment',{target_row_id:state.targetRow||'',comment:q('comment').value});
      await load(); setMessage('Comment saved','ok');
    } catch(err) { setMessage(String(err),'bad'); }
  });
  q('csv').addEventListener('change',async event=>{
    if(!event.target.value)return;
    try {
      await action('obslog_open_existing',{path:event.target.value}); await load(); setMessage('CSV opened','ok');
    } catch(err) { setMessage(String(err),'bad'); }
  });
  q('newCsv').addEventListener('click',async()=>{
    try { await action('obslog_new'); state.targetRow=null; await load(); setMessage('New CSV opened','ok'); }
    catch(err) { setMessage(String(err),'bad'); }
  });
  q('comment').addEventListener('keydown',event=>{if((event.metaKey||event.ctrlKey)&&event.key==='Enter'){event.preventDefault();q('save').click();}});
  document.addEventListener('click',event=>{
    const target=event.target;
    if(!target || typeof target.closest!=='function' || target.closest('.table-box,.controls,.editor'))return;
    clearSelection();
  },true);
  load();
})();
</script>
</body>
</html>"""
