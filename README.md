# Kita Drop: drop.kitatool.com

Send files and text between phones, tablets and computers on the same Wi-Fi. Runs in any modern browser (iPhone, Android, Windows, Mac, Linux). No install, no account, no upload to a server.

## How it works
- Devices that open the same page and share a **room code** (or scan the QR) see each other.
- The handshake goes through free public MQTT relays (names and connection info only, never your files).
- Files and text then travel **directly between the devices** over an encrypted WebRTC channel. On the same Wi-Fi that stays on your local network.

## Use it
1. Open `drop.kitatool.com` on both devices.
2. On one, tap **+** and scan the QR or open the link on the other (or turn on "Find devices on my network").
3. Tap the other device to send a file, or **Send text** for links and notes. The receiver taps Accept.
4. Optional: Add to Home Screen (iPhone Safari: Share, Add to Home Screen; Android or desktop Chrome: Install).

## Deploy (same pattern as kitatool.com)
1. New public GitHub repo, e.g. `kita-drop`, upload these files.
2. Settings, Pages, Deploy from a branch, main, / (root). `CNAME` already says `drop.kitatool.com`.
3. Spaceship, Advanced DNS for kitatool.com: add `CNAME`, host `drop`, value `droidkay.github.io`. Then tick Enforce HTTPS.

## Notes and limits
- Both devices need internet for the first handshake only. HTTPS is required (GitHub Pages provides it).
- Receiving: Chrome or Edge on a computer saves straight to disk, so file size doesn't matter. Other browsers hold the file in memory first, so very large files (over about 1 GB) are refused on those.
- Keep the page open on both devices while sending. Phones may pause transfers when the screen locks.
- Strict networks that block device-to-device traffic (some public or guest Wi-Fi) can stop a direct connection.
- The public relays are free and best-effort. The app talks to three at once, so one being down is fine. It can be pointed at your own relay later.
- Vendored libraries: mqtt.js 5.10.1, qrcode-generator 1.4.4.
