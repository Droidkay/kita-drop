"use strict";
/* Kita Drop: device-to-device transfer in the browser.
   - Devices find each other through a public MQTT relay (signalling only: names + WebRTC handshakes).
   - Files and text then go directly between the devices over a WebRTC data channel (encrypted).
   - On the same Wi-Fi that traffic stays on the local network. */

const $ = id => document.getElementById(id);
const LS = {
  get(k, d){ try{ const v = localStorage.getItem(k); return v === null ? d : v; }catch(e){ return d; } },
  set(k, v){ try{ localStorage.setItem(k, v); }catch(e){} }
};
const qs = new URLSearchParams(location.search);
const BROKERS = qs.get("broker") ? [qs.get("broker")] :
  ["wss://broker.emqx.io:8084/mqtt", "wss://broker.hivemq.com:8884/mqtt", "wss://test.mosquitto.org:8081"];
const ICE = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }, { urls: "stun:stun.cloudflare.com:3478" }] };
const MAX_MEM = 1.2 * 1024 * 1024 * 1024;     // largest file we'll hold in memory when we can't stream to disk
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const NS = "kd1";

const rnd = (n, set = ALPHABET) => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => set[b % set.length]).join("");
const sleep = ms => new Promise(r => setTimeout(r, ms));
function toast(m){ const t = $("toast"); t.textContent = m; t.classList.add("show"); clearTimeout(toast.t); toast.t = setTimeout(() => t.classList.remove("show"), 3200); }
function bytes(n){ if (n < 1024) return n + " B"; const u = ["KB","MB","GB","TB"]; let i = -1; do { n /= 1024; i++; } while (n >= 1024 && i < 3); return n.toFixed(n < 10 ? 1 : 0) + " " + u[i]; }

/* ---------------------------------------------------------------- identity */
const ADJ = ["Swift","Calm","Bright","Bold","Happy","Lucky","Cosmic","Mellow","Sunny","Brave","Gentle","Quick"];
const ANI = ["Falcon","Panda","Otter","Tiger","Koala","Eagle","Lynx","Fox","Whale","Gecko","Heron","Zebra"];
function deviceType(){
  const ua = navigator.userAgent;
  if (/iPad|Tablet/i.test(ua) || (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1)) return "tablet";
  if (/iPhone|Android.*Mobile|Mobile/i.test(ua)) return "phone";
  return "desktop";
}
const ICON = { phone: "📱", tablet: "📲", desktop: "💻" };
const me = {
  id: LS.get("kd_id", null) || rnd(10, "abcdefghijklmnopqrstuvwxyz0123456789"),
  name: LS.get("kd_name", null) || (ADJ[Math.floor(Math.random()*ADJ.length)] + " " + ANI[Math.floor(Math.random()*ANI.length)]),
  type: deviceType()
};
LS.set("kd_id", me.id); LS.set("kd_name", me.name);

/* ---------------------------------------------------------------- rooms */
let codeRoom = (qs.get("r") || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8) || LS.get("kd_room", null) || rnd(5);
LS.set("kd_room", codeRoom);
if (qs.get("r")) history.replaceState(null, "", location.pathname);
let nearbyOn = LS.get("kd_nearby", "0") === "1";
let nearbyRoom = null;

async function sha256hex(s){
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(d), b => b.toString(16).padStart(2, "0")).join("");
}
async function resolveNearby(){
  if (nearbyRoom) return nearbyRoom;
  for (const u of ["https://api.ipify.org?format=json", "https://api64.ipify.org?format=json"]) {
    try {
      const r = await fetch(u, { cache: "no-store" }); const j = await r.json();
      if (j.ip) { nearbyRoom = "n" + (await sha256hex("kd|" + j.ip)).slice(0, 16); return nearbyRoom; }
    } catch(e){}
  }
  return null;
}
const rooms = () => [codeRoom].concat(nearbyOn && nearbyRoom ? [nearbyRoom] : []);

/* ---------------------------------------------------------------- signalling (MQTT, several relays at once) */
const clients = [];
const seenMsgs = new Set();
function pub(topic, obj){
  obj.n = rnd(8, "abcdefghijklmnopqrstuvwxyz0123456789");
  const s = JSON.stringify(obj);
  for (const c of clients) if (c.connected) c.publish(topic, s, { qos: 0 });
}
function connectedCount(){ return clients.filter(c => c.connected).length; }
function updateNet(){
  const n = connectedCount(), chip = $("net");
  chip.className = "chip " + (n ? "ok" : "bad");
  $("netTxt").textContent = n ? "Online · room " + codeRoom : "Can't reach the relay";
}
function startBrokers(){
  for (const url of BROKERS) {
    let c;
    try { c = mqtt.connect(url, { clientId: "kd-" + me.id + "-" + rnd(4, "abcdefghij"), reconnectPeriod: 4000, connectTimeout: 8000, keepalive: 30, clean: true }); }
    catch(e){ continue; }
    clients.push(c);
    c.on("connect", () => { c.subscribe(NS + "/s/" + me.id); for (const r of rooms()) c.subscribe(NS + "/r/" + r); updateNet(); announce("hello"); });
    c.on("close", updateNet);
    c.on("error", () => {});
    c.on("message", (topic, payload) => {
      let m; try { m = JSON.parse(payload.toString()); } catch(e){ return; }
      if (!m || !m.n || seenMsgs.has(m.n)) return;
      seenMsgs.add(m.n); if (seenMsgs.size > 600) seenMsgs.delete(seenMsgs.values().next().value);
      onSignal(topic, m);
    });
  }
}
function subscribeRooms(){ for (const c of clients) if (c.connected) for (const r of rooms()) c.subscribe(NS + "/r/" + r); }
function announce(kind){
  for (const r of rooms()) pub(NS + "/r/" + r, { t: kind, id: me.id, name: me.name, type: me.type, room: r });
}

/* ---------------------------------------------------------------- peers */
const peers = new Map();
function touchPeer(m){
  if (!m.id || m.id === me.id) return null;
  let p = peers.get(m.id);
  if (!p) { p = { id: m.id, name: m.name || "Device", type: m.type || "desktop", seen: 0, nearby: !!m.room && m.room !== codeRoom, link: null, status: "" }; peers.set(m.id, p); }
  p.name = m.name || p.name; p.type = m.type || p.type; p.seen = Date.now();
  if (m.room === codeRoom) p.nearby = false;
  return p;
}
function onSignal(topic, m){
  if (m.id === me.id || m.from === me.id) return;
  switch (m.t) {
    case "hello": { const p = touchPeer(m); if (p) { pub(NS + "/s/" + m.id, { t: "here", id: me.id, name: me.name, type: me.type, room: m.room }); renderPeers(); } break; }
    case "here": { if (touchPeer(m)) renderPeers(); break; }
    case "bye": { const p = peers.get(m.id); if (p && !(p.link && p.link.open)) { peers.delete(m.id); renderPeers(); } break; }
    case "offer": case "answer": case "ice": handleRtcSignal(m); break;
  }
}
setInterval(() => {
  announce("here");
  const now = Date.now();
  let changed = false;
  for (const [id, p] of peers) if (now - p.seen > 75000 && !(p.link && p.link.open)) { peers.delete(id); changed = true; }
  if (changed) renderPeers();
}, 20000);
window.addEventListener("pagehide", () => announce("bye"));

/* ---------------------------------------------------------------- WebRTC link to one peer */
class Link {
  constructor(peer){
    this.peer = peer; this.pc = null; this.dc = null; this.open = false; this.sid = null; this.initiator = false;
    this.pendingIce = []; this.opening = null; this.out = null; this.inc = null; this.waiters = new Map(); this.queue = Promise.resolve();
    this.chunk = 16384;
  }
  reset(){
    try { this.dc && this.dc.close(); } catch(e){}
    try { this.pc && this.pc.close(); } catch(e){}
    this.pc = this.dc = null; this.open = false; this.pendingIce = []; this.opening = null;
  }
  makePc(){
    const pc = new RTCPeerConnection(ICE);
    pc.onicecandidate = e => { if (e.candidate) pub(NS + "/s/" + this.peer.id, { t: "ice", from: me.id, sid: this.sid, cand: e.candidate }); };
    pc.onconnectionstatechange = () => { if (["failed", "closed", "disconnected"].includes(pc.connectionState) && pc === this.pc) this.onClosed(); };
    pc.ondatachannel = e => this.attach(e.channel);
    this.pc = pc; return pc;
  }
  attach(dc){
    this.dc = dc; dc.binaryType = "arraybuffer"; dc.bufferedAmountLowThreshold = 1 << 20;
    const max = (this.pc && this.pc.sctp && this.pc.sctp.maxMessageSize) || 65536;
    this.chunk = Math.max(16384, Math.min(65536, max));
    dc.onopen = () => { this.open = true; setStatus(this.peer, ""); renderPeers(); if (this._resolve) this._resolve(); };
    dc.onclose = () => this.onClosed();
    dc.onmessage = e => this.onMessage(e.data);
  }
  onClosed(){
    if (!this.open && !this.opening) return;
    this.open = false;
    if (this.out) this.out.fail("Connection lost");
    if (this.inc) this.inc.fail("Connection lost");
    setStatus(this.peer, ""); renderPeers();
  }
  async ensureOpen(){
    if (this.open && this.dc && this.dc.readyState === "open") return;
    if (this.opening) return this.opening;
    this.reset();
    this.sid = rnd(6); this.initiator = true;
    this.opening = new Promise((res, rej) => {
      this._resolve = () => { this.opening = null; res(); };
      setTimeout(() => { if (!this.open) { this.opening = null; this.reset(); rej(new Error("Couldn't connect. Are both devices on the same Wi-Fi with Kita Drop open?")); } }, 15000);
    });
    const p = this.opening;
    const pc = this.makePc(); this.attach(pc.createDataChannel("kd", { ordered: true }));
    const offer = await pc.createOffer(); await pc.setLocalDescription(offer);
    pub(NS + "/s/" + this.peer.id, { t: "offer", from: me.id, sid: this.sid, sdp: pc.localDescription });
    return p;
  }
  async onRtc(m){
    if (m.t === "offer") {
      if (this.initiator && this.opening && me.id < m.from) return;     // we win the tie-break, ignore their offer
      this.reset(); this.initiator = false; this.sid = m.sid;
      const pc = this.makePc();
      await pc.setRemoteDescription(m.sdp);
      for (const c of this.pendingIce.splice(0)) { try { await pc.addIceCandidate(c); } catch(e){} }
      const ans = await pc.createAnswer(); await pc.setLocalDescription(ans);
      pub(NS + "/s/" + this.peer.id, { t: "answer", from: me.id, sid: this.sid, sdp: pc.localDescription });
    } else if (m.t === "answer") {
      if (!this.pc || m.sid !== this.sid) return;
      await this.pc.setRemoteDescription(m.sdp);
      for (const c of this.pendingIce.splice(0)) { try { await this.pc.addIceCandidate(c); } catch(e){} }
    } else if (m.t === "ice") {
      if (m.sid !== this.sid) { return; }
      if (this.pc && this.pc.remoteDescription) { try { await this.pc.addIceCandidate(m.cand); } catch(e){} }
      else this.pendingIce.push(m.cand);
    }
  }
  sendCtl(o){ this.dc.send(JSON.stringify(o)); }
  waitFor(key, ms = 180000){
    return new Promise((res, rej) => {
      const t = setTimeout(() => { this.waiters.delete(key); rej(new Error("No answer from the other device")); }, ms);
      this.waiters.set(key, v => { clearTimeout(t); this.waiters.delete(key); res(v); });
    });
  }
  /* ----- receiving ----- */
  onMessage(data){
    if (typeof data !== "string") { if (this.inc) this.inc.push(data); return; }
    let m; try { m = JSON.parse(data); } catch(e){ return; }
    switch (m.k) {
      case "offer": askIncoming(this, m); break;
      case "accept": case "reject": case "done": { const w = this.waiters.get(m.k + ":" + m.id); if (w) w(m); break; }
      case "cancel": if (this.inc && this.inc.id === m.id) this.inc.fail("Cancelled by sender"); if (this.out && this.out.id === m.id) this.out.cancelled = true; break;
      case "end": if (this.inc && this.inc.id === m.id) this.inc.finish(); break;
      case "text": addReceivedText(this.peer, m.text); break;
    }
  }
  /* ----- sending ----- */
  sendFile(file){
    this.queue = this.queue.then(() => this._sendFile(file)).catch(() => {});
    return this.queue;
  }
  async _sendFile(file){
    const x = newXfer("out", this.peer, file.name, file.size);
    try {
      setStatus(this.peer, "Connecting…");
      await this.ensureOpen();
      x.state("Waiting for " + this.peer.name + " to accept…");
      const id = rnd(8); x.id = id; this.out = x;
      this.sendCtl({ k: "offer", id, name: file.name, size: file.size, mime: file.type || "" });
      const reply = await Promise.race([this.waitFor("accept:" + id), this.waitFor("reject:" + id).then(r => { throw new Error(r.reason || "Declined"); })]);
      x.state("Sending…");
      setStatus(this.peer, "Sending…");
      let off = 0; const dc = this.dc, CH = this.chunk, t0 = performance.now();
      while (off < file.size) {
        if (x.cancelled) throw new Error("Cancelled");
        if (!this.open) throw new Error("Connection lost");
        if (dc.bufferedAmount > 8 * 1024 * 1024) {
          await new Promise(r => { const f = () => { dc.removeEventListener("bufferedamountlow", f); r(); }; dc.addEventListener("bufferedamountlow", f); });
        }
        const buf = await file.slice(off, off + CH).arrayBuffer();
        dc.send(buf); off += buf.byteLength;
        x.progress(off, file.size, (off / ((performance.now() - t0) / 1000)));
      }
      this.sendCtl({ k: "end", id });
      x.state("Finishing…");
      await this.waitFor("done:" + id, 600000);
      x.complete("Sent");
    } catch (e) {
      try { if (this.open && x.id && e.message === "Cancelled") this.sendCtl({ k: "cancel", id: x.id }); } catch(_){}
      x.fail(e.message || "Failed");
    } finally { this.out = null; setStatus(this.peer, ""); }
  }
  async sendText(text){
    try { setStatus(this.peer, "Connecting…"); await this.ensureOpen(); this.sendCtl({ k: "text", text }); toast("Sent to " + this.peer.name); }
    catch (e) { toast(e.message); }
    setStatus(this.peer, "");
  }
}
function linkFor(peer){ return peer.link || (peer.link = new Link(peer)); }
function handleRtcSignal(m){
  let p = peers.get(m.from);
  if (!p) { p = touchPeer({ id: m.from, name: "Device", type: "desktop", room: codeRoom }); renderPeers(); }
  linkFor(p).onRtc(m).catch(() => {});
}
function setStatus(peer, s){ peer.status = s; renderPeers(); }

/* ---------------------------------------------------------------- incoming transfers */
const askQueue = []; let asking = false;
function askIncoming(link, m){ askQueue.push({ link, m }); if (!asking) nextAsk(); }
function nextAsk(){
  const item = askQueue.shift(); if (!item) { asking = false; return; }
  asking = true;
  const { link, m } = item, peer = link.peer, size = +m.size || 0;
  const tooBig = !window.showSaveFilePicker && size > MAX_MEM;
  if (tooBig) { link.sendCtl({ k: "reject", id: m.id, reason: "Too big for this device's browser. Use Chrome or Edge on a computer to receive it." }); asking = false; nextAsk(); return; }
  $("inIcon").textContent = "📥"; $("inTitle").textContent = peer.name + " wants to send you a file";
  $("inBody").textContent = m.name + " · " + bytes(size);
  $("incoming").classList.add("open");
  const done = () => { $("incoming").classList.remove("open"); asking = false; nextAsk(); };
  $("inNo").onclick = () => { link.sendCtl({ k: "reject", id: m.id, reason: "Declined" }); done(); };
  $("inYes").onclick = async () => {
    // must start the save dialog inside this click so the browser allows it
    let handle = null;
    if (window.showSaveFilePicker) {
      try { handle = await window.showSaveFilePicker({ suggestedName: m.name }); }
      catch (e) { link.sendCtl({ k: "reject", id: m.id, reason: "Declined" }); done(); return; }
    }
    done();
    startReceive(link, m, handle);
  };
}
async function startReceive(link, m, handle){
  const x = newXfer("in", link.peer, m.name, +m.size || 0); x.id = m.id;
  let writer = null; const parts = []; let got = 0, chain = Promise.resolve(), t0 = performance.now();
  if (handle) { try { writer = await handle.createWritable(); } catch(e){ link.sendCtl({ k: "reject", id: m.id, reason: "Couldn't write the file" }); x.fail("Couldn't write the file"); return; } }
  const inc = {
    id: m.id,
    push(buf){
      got += buf.byteLength;
      if (writer) chain = chain.then(() => writer.write(buf)); else parts.push(buf);
      x.progress(got, x.size, got / ((performance.now() - t0) / 1000));
    },
    async finish(){
      try {
        await chain;
        if (writer) { await writer.close(); x.complete("Saved"); }
        else {
          const blob = new Blob(parts, { type: m.mime || "application/octet-stream" });
          const url = URL.createObjectURL(blob); x.complete("Received");
          addReceivedFile(link.peer, m.name, blob.size, url);
          const a = document.createElement("a"); a.href = url; a.download = m.name; document.body.appendChild(a); a.click(); a.remove();
        }
        link.sendCtl({ k: "done", id: m.id });
      } catch (e) { x.fail("Couldn't save the file"); }
      link.inc = null; setStatus(link.peer, "");
    },
    fail(msg){ try { writer && writer.abort(); } catch(e){} x.fail(msg); link.inc = null; setStatus(link.peer, ""); }
  };
  x.cancel = () => { try { link.sendCtl({ k: "cancel", id: m.id }); } catch(e){} inc.fail("Cancelled"); };
  link.inc = inc; setStatus(link.peer, "Receiving…");
  link.sendCtl({ k: "accept", id: m.id });
}

/* ---------------------------------------------------------------- transfer list UI */
function newXfer(dir, peer, name, size){
  $("xferPanel").classList.remove("hidden");
  const row = document.createElement("div"); row.className = "row";
  row.innerHTML = '<div class="ic"></div><div class="meta"><div class="nm"></div><div class="sub"></div><div class="bar"><i></i></div></div><button class="sm hidden">Cancel</button>';
  row.querySelector(".ic").textContent = dir === "out" ? "📤" : "📥";
  row.querySelector(".nm").textContent = name;
  const sub = row.querySelector(".sub"), bar = row.querySelector(".bar"), fill = bar.querySelector("i"), btn = row.querySelector("button");
  sub.textContent = (dir === "out" ? "To " : "From ") + peer.name + " · " + bytes(size);
  $("xfers").prepend(row);
  let last = 0;
  const x = {
    id: null, size, dir, cancelled: false, finished: false,
    state(s){ sub.textContent = (dir === "out" ? "To " : "From ") + peer.name + " · " + s; btn.classList.remove("hidden"); },
    progress(done, total, rate){
      const now = performance.now(); if (now - last < 120 && done < total) return; last = now;
      const pc = total ? Math.min(100, done / total * 100) : 0; fill.style.width = pc + "%";
      sub.textContent = (dir === "out" ? "To " : "From ") + peer.name + " · " + bytes(done) + " of " + bytes(total) + " · " + bytes(rate) + "/s";
      btn.classList.remove("hidden");
    },
    complete(s){ x.finished = true; fill.style.width = "100%"; bar.classList.add("done"); sub.textContent = (dir === "out" ? "To " : "From ") + peer.name + " · " + s + " · " + bytes(size); btn.classList.add("hidden"); },
    fail(msg){ if (x.finished) return; x.finished = true; bar.classList.add("fail"); sub.textContent = msg; btn.classList.add("hidden"); },
    cancel(){ x.cancelled = true; }
  };
  btn.onclick = () => x.cancel();
  return x;
}
function addReceivedFile(peer, name, size, url){
  $("recvPanel").classList.remove("hidden");
  const row = document.createElement("div"); row.className = "row";
  row.innerHTML = '<div class="ic">📄</div><div class="meta"><div class="nm"></div><div class="sub"></div></div><a class="sm">Save</a>';
  row.querySelector(".nm").textContent = name; row.querySelector(".sub").textContent = "From " + peer.name + " · " + bytes(size);
  const a = row.querySelector("a"); a.href = url; a.download = name;
  $("recvs").prepend(row);
}
function addReceivedText(peer, text){
  $("recvPanel").classList.remove("hidden");
  const row = document.createElement("div"); row.className = "row";
  row.innerHTML = '<div class="ic">💬</div><div class="meta"><div class="sub"></div><div class="txt"></div></div><button class="sm">Copy</button>';
  row.querySelector(".sub").textContent = "Text from " + peer.name; row.querySelector(".txt").textContent = text;
  row.querySelector("button").onclick = async () => { try { await navigator.clipboard.writeText(text); toast("Copied"); } catch(e){ toast("Press and hold the text to copy"); } };
  if (/^https?:\/\/\S+$/.test(text.trim())) { const o = document.createElement("a"); o.className = "sm"; o.textContent = "Open"; o.href = text.trim(); o.target = "_blank"; o.rel = "noopener noreferrer"; row.appendChild(o); }
  $("recvs").prepend(row);
  toast("Text from " + peer.name);
}

/* ---------------------------------------------------------------- peers UI */
let pickFor = null, textFor = null;
function renderPeers(){
  const box = $("peers"); box.innerHTML = "";
  const list = [...peers.values()].sort((a, b) => a.name.localeCompare(b.name));
  $("hint").classList.toggle("hidden", list.length > 0);
  document.body.classList.toggle("has-peers", list.length > 0);
  for (const p of list) {
    const el = document.createElement("div"); el.className = "peer"; el.dataset.id = p.id;
    el.innerHTML = '<span class="tag"></span><div class="avatar"></div><b></b><div class="st"></div><div class="drop">Tap or drop files here</div><button class="tbtn">Send text</button>';
    el.querySelector(".tag").textContent = p.nearby ? "nearby" : "";
    el.querySelector(".avatar").textContent = ICON[p.type] || "💻";
    el.querySelector("b").textContent = p.name;
    const st = el.querySelector(".st"); st.textContent = p.status || (p.link && p.link.open ? "Connected" : "Ready"); if (p.status) st.classList.add("live");
    el.onclick = e => { if (e.target.closest(".tbtn")) return; pickFor = p; $("picker").value = ""; $("picker").click(); };
    el.querySelector(".tbtn").onclick = () => { textFor = p; $("textTitle").textContent = "Send text to " + p.name; $("textIn").value = ""; $("textModal").classList.add("open"); setTimeout(() => $("textIn").focus(), 50); };
    el.addEventListener("dragover", e => { e.preventDefault(); el.classList.add("drag"); });
    el.addEventListener("dragleave", () => el.classList.remove("drag"));
    el.addEventListener("drop", e => { e.preventDefault(); e.stopPropagation(); el.classList.remove("drag"); $("veil").classList.remove("on"); sendMany(p, e.dataTransfer.files); });
    box.appendChild(el);
  }
}
function sendMany(peer, files){
  const arr = Array.from(files || []); if (!arr.length) return;
  const l = linkFor(peer); for (const f of arr) l.sendFile(f);
}
$("picker").addEventListener("change", e => { if (pickFor) sendMany(pickFor, e.target.files); });
$("textCancel").onclick = () => $("textModal").classList.remove("open");
$("textSend").onclick = () => { const t = $("textIn").value; if (t.trim() && textFor) linkFor(textFor).sendText(t); $("textModal").classList.remove("open"); };
let dragDepth = 0;
window.addEventListener("dragenter", e => { if (e.dataTransfer && [...e.dataTransfer.types].includes("Files")) { dragDepth++; $("veil").classList.add("on"); } });
window.addEventListener("dragleave", () => { dragDepth = Math.max(0, dragDepth - 1); if (!dragDepth) $("veil").classList.remove("on"); });
window.addEventListener("dragover", e => e.preventDefault());
window.addEventListener("drop", e => { e.preventDefault(); dragDepth = 0; $("veil").classList.remove("on"); const l = [...peers.values()]; if (l.length === 1 && e.dataTransfer.files.length) sendMany(l[0], e.dataTransfer.files); });
window.addEventListener("paste", e => {
  if (/input|textarea/i.test((document.activeElement || {}).tagName || "")) return;
  const t = e.clipboardData && e.clipboardData.getData("text"); const l = [...peers.values()];
  if (t && l.length === 1) { linkFor(l[0]).sendText(t); }
});

/* me */
function renderMe(){ $("meName").textContent = me.name; $("meIcon").textContent = ICON[me.type] || "💻"; }
$("meBtn").onclick = () => {
  const n = prompt("Name this device", me.name); if (!n || !n.trim()) return;
  me.name = n.trim().slice(0, 24); LS.set("kd_name", me.name); renderMe(); announce("hello");
};

/* invite / room */
function inviteUrl(){ return location.origin + location.pathname + "?r=" + codeRoom; }
function renderInvite(){
  let svg = "";
  try { const q = qrcode(0, "M"); q.addData(inviteUrl()); q.make(); svg = q.createSvgTag({ cellSize: 4, margin: 0, scalable: true }); } catch(e){}
  document.querySelectorAll(".qr").forEach(e => { e.innerHTML = svg; });
  document.querySelectorAll(".roomcode").forEach(e => { e.textContent = codeRoom; });
}
function openRoom(){ renderInvite(); $("nearby").checked = nearbyOn; $("roomModal").classList.add("open"); }
$("inviteBtn").onclick = openRoom;
$("roomDone").onclick = () => $("roomModal").classList.remove("open");
$("roomModal").addEventListener("click", e => { if (e.target.id === "roomModal") $("roomModal").classList.remove("open"); });
function switchRoom(code){
  const old = codeRoom;
  announce("bye");
  for (const c of clients) if (c.connected && old !== code) c.unsubscribe(NS + "/r/" + old);
  codeRoom = code; LS.set("kd_room", codeRoom);
  for (const [id, p] of peers) if (!(p.link && p.link.open)) peers.delete(id);
  subscribeRooms(); announce("hello"); renderPeers(); renderInvite(); updateNet();
}
function joinFromInput(box){
  const inp = box.querySelector(".codein");
  const v = (inp.value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (v.length < 4) { toast("Enter the room code (4 to 8 letters or numbers)"); inp.focus(); return; }
  switchRoom(v); inp.value = "";
  box.querySelector('[data-tab="share"]').click();
  $("roomModal").classList.remove("open");
  toast("Joined room " + codeRoom);
}
document.querySelectorAll(".cmount").forEach(box => {
  box.appendChild($("tplConnect").content.cloneNode(true));
  const tabs = box.querySelectorAll("[data-tab]");
  tabs.forEach(t => t.onclick = () => {
    tabs.forEach(x => x.classList.toggle("on", x === t));
    box.querySelector(".pane-share").classList.toggle("hidden", t.dataset.tab !== "share");
    box.querySelector(".pane-join").classList.toggle("hidden", t.dataset.tab !== "join");
    if (t.dataset.tab === "join") setTimeout(() => box.querySelector(".codein").focus(), 50);
  });
  box.querySelector(".js-copy").onclick = async () => { try { await navigator.clipboard.writeText(inviteUrl()); toast("Link copied"); } catch(e){ toast(inviteUrl()); } };
  box.querySelector(".js-newroom").onclick = () => { switchRoom(rnd(5)); toast("New room " + codeRoom + " created"); };
  box.querySelector(".js-join").onclick = () => joinFromInput(box);
  const inp = box.querySelector(".codein");
  inp.addEventListener("input", () => { inp.value = inp.value.toUpperCase().replace(/[^A-Z0-9]/g, ""); });
  inp.addEventListener("keydown", e => { if (e.key === "Enter") joinFromInput(box); });
});
document.querySelectorAll(".js-more").forEach(b => { b.onclick = openRoom; });

$("nearby").onchange = async e => {
  nearbyOn = e.target.checked; LS.set("kd_nearby", nearbyOn ? "1" : "0");
  if (nearbyOn) { const r = await resolveNearby(); if (!r) { toast("Couldn't work out your network. Check your internet."); nearbyOn = false; e.target.checked = false; LS.set("kd_nearby", "0"); return; } subscribeRooms(); announce("hello"); }
  else { announce("bye"); for (const [id, p] of peers) if (p.nearby && !(p.link && p.link.open)) peers.delete(id); renderPeers(); }
};

/* ---------------------------------------------------------------- start */
(async function init(){
  renderMe(); renderPeers(); renderInvite();
  $("hintUrl").textContent = location.host + (location.pathname.replace(/\/$/, "") || "");
  if (nearbyOn) await resolveNearby();
  startBrokers(); updateNet();
  if ("serviceWorker" in navigator && location.protocol === "https:") navigator.serviceWorker.register("sw.js").catch(() => {});
  if (qs.get("r")) setTimeout(() => toast("Joined room " + codeRoom), 600);
})();
window.__kd = { me, peers, rooms, get codeRoom(){ return codeRoom; } };
