"use strict";
/* Kita Clips (web): a shared clipboard for your devices.
   Same room + WebRTC mesh as Kita Drop. Every device in a room connects to every other one,
   and a clip you share is sent over the encrypted data channels to all of them. */
const $ = id => document.getElementById(id);
const LS = {
  get(k, d){ try{ const v = localStorage.getItem(k); return v === null ? d : v; }catch(e){ return d; } },
  set(k, v){ try{ localStorage.setItem(k, v); }catch(e){} }
};
const qs = new URLSearchParams(location.search);
const BROKERS = qs.get("broker") ? [qs.get("broker")] :
  ["wss://broker.emqx.io:8084/mqtt", "wss://broker.hivemq.com:8884/mqtt", "wss://test.mosquitto.org:8081"];
const ICE = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }, { urls: "stun:stun.cloudflare.com:3478" }] };
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const NS = "kc1";
const MAX_CLIPS = 200, MAX_LEN = 200000;
const rnd = (n, set = ALPHABET) => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => set[b % set.length]).join("");
function toast(m){ const t = $("toast"); t.textContent = m; t.classList.add("show"); clearTimeout(toast.t); toast.t = setTimeout(() => t.classList.remove("show"), 3000); }
const esc = s => s.replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const ADJ = ["Swift","Calm","Bright","Bold","Happy","Lucky","Cosmic","Mellow","Sunny","Brave","Gentle","Quick"];
const ANI = ["Falcon","Panda","Otter","Tiger","Koala","Eagle","Lynx","Fox","Whale","Gecko","Heron","Zebra"];
const me = {
  id: LS.get("kc_id", null) || rnd(10, "abcdefghijklmnopqrstuvwxyz0123456789"),
  name: LS.get("kc_name", null) || (ADJ[Math.floor(Math.random()*ADJ.length)] + " " + ANI[Math.floor(Math.random()*ANI.length)])
};
LS.set("kc_id", me.id); LS.set("kc_name", me.name);

/* ---------------- room */
let codeRoom = (qs.get("r") || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8) || LS.get("kc_room", null) || rnd(5);
LS.set("kc_room", codeRoom);
if (qs.get("r")) history.replaceState(null, "", location.pathname);

/* ---------------- signalling */
const clients = [], seen = new Set();
function pub(topic, obj){
  obj.n = rnd(8, "abcdefghijklmnopqrstuvwxyz0123456789");
  const s = JSON.stringify(obj);
  for (const c of clients) if (c.connected) c.publish(topic, s, { qos: 0 });
}
const online = () => clients.filter(c => c.connected).length;
function updateNet(){ const n = online(); $("net").className = "chip " + (n ? "ok" : "bad"); $("netTxt").textContent = n ? "Online · room " + codeRoom : "Can't reach the relay"; }
function announce(kind){ pub(NS + "/r/" + codeRoom, { t: kind, id: me.id, name: me.name }); }
function startBrokers(){
  for (const url of BROKERS) {
    let c; try { c = mqtt.connect(url, { clientId: "kc-" + me.id + "-" + rnd(4, "abcdefghij"), reconnectPeriod: 4000, connectTimeout: 8000, keepalive: 30, clean: true }); } catch(e){ continue; }
    clients.push(c);
    c.on("connect", () => { c.subscribe(NS + "/s/" + me.id); c.subscribe(NS + "/r/" + codeRoom); updateNet(); announce("hello"); });
    c.on("close", updateNet); c.on("error", () => {});
    c.on("message", (topic, payload) => {
      let m; try { m = JSON.parse(payload.toString()); } catch(e){ return; }
      if (!m || !m.n || seen.has(m.n)) return;
      seen.add(m.n); if (seen.size > 600) seen.delete(seen.values().next().value);
      onSignal(m);
    });
  }
}

/* ---------------- peers + links (auto-connect; lower id dials) */
const peers = new Map();
function touch(m){
  if (!m.id || m.id === me.id) return null;
  let p = peers.get(m.id);
  if (!p) { p = { id: m.id, name: m.name || "Device", seen: 0, link: null }; peers.set(m.id, p); }
  p.name = m.name || p.name; p.seen = Date.now();
  if (me.id < p.id) linkFor(p).dial();
  return p;
}
function onSignal(m){
  if (m.id === me.id || m.from === me.id) return;
  if (m.t === "hello") { if (touch(m)) { pub(NS + "/s/" + m.id, { t: "here", id: me.id, name: me.name }); renderPeers(); } }
  else if (m.t === "here") { if (touch(m)) renderPeers(); }
  else if (m.t === "bye") { const p = peers.get(m.id); if (p && !(p.link && p.link.open)) { peers.delete(m.id); renderPeers(); } }
  else if (m.t === "offer" || m.t === "answer" || m.t === "ice") {
    let p = peers.get(m.from); if (!p) { p = { id: m.from, name: "Device", seen: Date.now(), link: null }; peers.set(m.from, p); }
    linkFor(p).onRtc(m).catch(() => {});
  }
}
setInterval(() => {
  announce("here");
  const now = Date.now(); let ch = false;
  for (const [id, p] of peers) {
    if (now - p.seen > 75000 && !(p.link && p.link.open)) { peers.delete(id); ch = true; }
    else if (me.id < id && p.link && !p.link.open && !p.link.busy) p.link.dial();
  }
  if (ch) renderPeers();
}, 20000);
addEventListener("pagehide", () => announce("bye"));

class Link {
  constructor(p){ this.p = p; this.pc = null; this.dc = null; this.open = false; this.busy = false; this.sid = null; this.ice = []; }
  reset(){ try{ this.dc && this.dc.close(); }catch(e){} try{ this.pc && this.pc.close(); }catch(e){} this.pc = this.dc = null; this.open = false; this.busy = false; this.ice = []; }
  makePc(){
    const pc = new RTCPeerConnection(ICE);
    pc.onicecandidate = e => { if (e.candidate) pub(NS + "/s/" + this.p.id, { t: "ice", from: me.id, sid: this.sid, cand: e.candidate }); };
    pc.onconnectionstatechange = () => { if (pc === this.pc && ["failed", "closed", "disconnected"].includes(pc.connectionState)) { this.open = false; this.busy = false; renderPeers(); } };
    pc.ondatachannel = e => this.attach(e.channel);
    this.pc = pc; return pc;
  }
  attach(dc){
    this.dc = dc;
    dc.onopen = () => { this.open = true; this.busy = false; renderPeers(); this.send({ k: "hi", name: me.name }); };
    dc.onclose = () => { this.open = false; this.busy = false; renderPeers(); };
    dc.onmessage = e => { let m; try { m = JSON.parse(e.data); } catch(_){ return; } if (m.k === "clip") addClip({ id: m.id, text: m.text, from: this.p.name, ts: m.ts || Date.now() }, true); else if (m.k === "hi" && m.name) { this.p.name = m.name; renderPeers(); } };
  }
  async dial(){
    if (this.open || this.busy) return;
    this.reset(); this.busy = true; this.sid = rnd(6);
    setTimeout(() => { if (!this.open && this.busy) { this.reset(); } }, 15000);
    try {
      const pc = this.makePc(); this.attach(pc.createDataChannel("kc"));
      await pc.setLocalDescription(await pc.createOffer());
      pub(NS + "/s/" + this.p.id, { t: "offer", from: me.id, sid: this.sid, sdp: pc.localDescription });
    } catch(e){ this.reset(); }
  }
  async onRtc(m){
    if (m.t === "offer") {
      this.reset(); this.sid = m.sid; this.busy = true;
      const pc = this.makePc(); await pc.setRemoteDescription(m.sdp);
      for (const c of this.ice.splice(0)) { try { await pc.addIceCandidate(c); } catch(e){} }
      await pc.setLocalDescription(await pc.createAnswer());
      pub(NS + "/s/" + this.p.id, { t: "answer", from: me.id, sid: this.sid, sdp: pc.localDescription });
    } else if (m.t === "answer") {
      if (!this.pc || m.sid !== this.sid) return;
      await this.pc.setRemoteDescription(m.sdp);
      for (const c of this.ice.splice(0)) { try { await this.pc.addIceCandidate(c); } catch(e){} }
    } else if (m.t === "ice") {
      if (m.sid !== this.sid) return;
      if (this.pc && this.pc.remoteDescription) { try { await this.pc.addIceCandidate(m.cand); } catch(e){} } else this.ice.push(m.cand);
    }
  }
  send(o){ if (this.open && this.dc && this.dc.readyState === "open") { this.dc.send(JSON.stringify(o)); return true; } return false; }
}
function linkFor(p){ return p.link || (p.link = new Link(p)); }

function renderPeers(){
  const all = [...peers.values()], up = all.filter(p => p.link && p.link.open);
  $("peerLine").textContent = !all.length ? "No other devices in this room yet." :
    up.length ? "Connected: " + up.map(p => p.name).join(", ") + (up.length < all.length ? " · connecting " + (all.length - up.length) + "…" : "") :
    "Found " + all.length + " device" + (all.length > 1 ? "s" : "") + ", connecting…";
}

/* ---------------- clips */
let clips = []; try { clips = JSON.parse(LS.get("kc_clips", "[]")) || []; } catch(e){ clips = []; }
const save = () => { while (true) { try { localStorage.setItem("kc_clips", JSON.stringify(clips)); return; } catch(e){ if (clips.length < 2) return; const i = clips.map(c => c.pin).lastIndexOf(false); clips.splice(i < 0 ? clips.length - 1 : i, 1); } } };
let lastSeenText = LS.get("kc_last", "");
function addClip(c, incoming){
  if (!c.text || !c.text.trim()) return;
  c.text = c.text.slice(0, MAX_LEN); c.id = c.id || rnd(10, "abcdefghijklmnopqrstuvwxyz0123456789");
  if (clips.some(x => x.id === c.id)) return;
  c.pin = false; clips.unshift(c);
  const unpinned = clips.filter(x => !x.pin); if (unpinned.length > MAX_CLIPS) clips = clips.filter(x => x.pin || unpinned.indexOf(x) < MAX_CLIPS);
  save(); render();
  if (incoming) {
    toast("New clip from " + c.from);
    try { navigator.clipboard.writeText(c.text).then(() => { lastSeenText = c.text; LS.set("kc_last", lastSeenText); }, () => {}); } catch(e){}
  }
}
function shareClip(text){
  if (!text || !text.trim()) { toast("Nothing to share"); return false; }
  const c = { id: rnd(10, "abcdefghijklmnopqrstuvwxyz0123456789"), text: text.slice(0, MAX_LEN), from: "This device", ts: Date.now() };
  addClip(c, false);
  let n = 0; for (const p of peers.values()) if (p.link && p.link.send({ k: "clip", id: c.id, text: c.text, ts: c.ts })) n++;
  toast(n ? "Shared with " + n + " device" + (n > 1 ? "s" : "") : "Saved here. No other device is connected yet.");
  return true;
}
function ago(ts){ const s = Math.max(0, (Date.now() - ts) / 1000 | 0); if (s < 60) return "just now"; if (s < 3600) return (s / 60 | 0) + " min ago"; if (s < 86400) return (s / 3600 | 0) + " h ago"; return new Date(ts).toLocaleDateString(); }
function linkify(t){ return esc(t).replace(/(https?:\/\/[^\s<]+)/g, '<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>'); }
function render(){
  const q = $("search").value.trim().toLowerCase();
  const list = clips.filter(c => !q || c.text.toLowerCase().includes(q)).sort((a, b) => (b.pin - a.pin) || (b.ts - a.ts));
  $("empty").classList.toggle("hidden", !!clips.length);
  $("clips").innerHTML = list.map(c => `<div class="clip${c.pin ? " pin" : ""}" data-id="${c.id}"><div class="meta"><b>${esc(c.from)}</b> · ${ago(c.ts)}${c.pin ? " · 📌" : ""}</div><div class="body">${linkify(c.text)}</div><div class="acts"><button class="p" data-a="copy">Copy</button><button data-a="pin">${c.pin ? "Unpin" : "Pin"}</button><button data-a="share">Re-share</button><button data-a="del">Delete</button></div></div>`).join("");
}
$("clips").onclick = e => {
  const b = e.target.closest("button[data-a]"); if (!b) return;
  const el = b.closest(".clip"), c = clips.find(x => x.id === el.dataset.id); if (!c) return;
  const a = b.dataset.a;
  if (a === "copy") navigator.clipboard.writeText(c.text).then(() => { lastSeenText = c.text; LS.set("kc_last", lastSeenText); toast("Copied"); }, () => toast("Couldn't copy. Select the text and copy it."));
  else if (a === "pin") { c.pin = !c.pin; save(); render(); }
  else if (a === "del") { clips = clips.filter(x => x !== c); save(); render(); }
  else if (a === "share") { let n = 0; for (const p of peers.values()) if (p.link && p.link.send({ k: "clip", id: rnd(10, "abcdefghijklmnopqrstuvwxyz0123456789"), text: c.text, ts: Date.now() })) n++; toast(n ? "Shared again" : "No other device connected"); }
};
$("sendBtn").onclick = () => { if (shareClip($("box").value)) $("box").value = ""; };
$("box").addEventListener("keydown", e => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) $("sendBtn").click(); });
$("pasteBtn").onclick = async () => { try { const t = await navigator.clipboard.readText(); if (t) shareClip(t); else toast("Clipboard is empty"); } catch(e){ toast("Allow clipboard access, or paste into the box instead."); $("box").focus(); } };
$("search").oninput = render;
$("clearBtn").onclick = () => { if (!clips.length) return; if (confirm("Delete all clips on this device? Pinned ones too.")) { clips = []; save(); render(); } };
$("auto").checked = LS.get("kc_auto", "0") === "1";
$("auto").onchange = () => { LS.set("kc_auto", $("auto").checked ? "1" : "0"); if ($("auto").checked) grab(); };
async function grab(){
  if (!$("auto").checked || document.hidden) return;
  try { const t = await navigator.clipboard.readText(); if (t && t !== lastSeenText) { lastSeenText = t; LS.set("kc_last", t); shareClip(t); } } catch(e){}
}
document.addEventListener("visibilitychange", grab); addEventListener("focus", grab);
setInterval(render, 60000);

/* ---------------- rooms UI (create / join, QR, manual code) */
function inviteUrl(){ return location.origin + location.pathname + "?r=" + codeRoom; }
function renderInvite(){
  let svg = ""; try { const q = qrcode(0, "M"); q.addData(inviteUrl()); q.make(); svg = q.createSvgTag({ cellSize: 4, margin: 0, scalable: true }); } catch(e){}
  document.querySelectorAll(".qr").forEach(e => { e.innerHTML = svg; });
  document.querySelectorAll(".roomcode").forEach(e => { e.textContent = codeRoom; });
}
function switchRoom(code){
  const old = codeRoom; announce("bye");
  for (const c of clients) if (c.connected && old !== code) c.unsubscribe(NS + "/r/" + old);
  codeRoom = code; LS.set("kc_room", codeRoom);
  for (const [id, p] of peers) { if (p.link) p.link.reset(); peers.delete(id); }
  for (const c of clients) if (c.connected) c.subscribe(NS + "/r/" + codeRoom);
  announce("hello"); renderPeers(); renderInvite(); updateNet();
}
function joinFrom(box){
  const inp = box.querySelector(".codein"), v = (inp.value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (v.length < 4) { toast("Enter the room code (4 to 8 letters or numbers)"); inp.focus(); return; }
  switchRoom(v); inp.value = ""; box.querySelector('[data-tab="share"]').click(); $("roomModal").classList.remove("open"); toast("Joined room " + codeRoom);
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
  box.querySelector(".js-join").onclick = () => joinFrom(box);
  const inp = box.querySelector(".codein");
  inp.addEventListener("input", () => { inp.value = inp.value.toUpperCase().replace(/[^A-Z0-9]/g, ""); });
  inp.addEventListener("keydown", e => { if (e.key === "Enter") joinFrom(box); });
});
$("inviteBtn").onclick = () => { renderInvite(); $("roomModal").classList.add("open"); };
$("roomDone").onclick = () => $("roomModal").classList.remove("open");
$("roomModal").addEventListener("click", e => { if (e.target.id === "roomModal") $("roomModal").classList.remove("open"); });

renderInvite(); render(); renderPeers(); startBrokers(); updateNet();
