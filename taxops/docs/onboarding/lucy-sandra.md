# TaxOps onboarding — Lucy & Sandra

**For:** Lucy (`lucy`) and Sandra (`sandra`)  
**Role:** Preparer  
**Date:** September 2026

Hand this sheet to each person when they first sign in. Temporary password is below — **change it immediately** when TaxOps asks.

---

## 1. Open TaxOps

| | |
|---|---|
| **URL** | http://192.168.1.173:5000 |
| **Or** | http://taxlog:5000 (after workstation setup) |
| **Desktop** | “Tax Log” shortcut (if setup already ran) |

If the page does not load: confirm network drives (especially **T:**) with `\\Xcel-server\taxops\SETUP_WORKSTATION.bat`, or ask Moises / admin.

---

## 2. Your login

| Name | Username | Temporary password |
|------|----------|--------------------|
| Lucy | `lucy` | `temp@92` |
| Sandra | `sandra` | `temp@92` |

1. Sign in with your username and **`temp@92`**.
2. TaxOps will require a **new password** — pick one only you know (and that meets the length rules on screen).
3. You may see a one-time **Welcome** screen listing menus — click through once.
4. Optional: start the **Guided Tour** from the help icon in the header anytime.

Do **not** share `temp@92` after you change it. If you get locked out, ask an admin to reset you again.

---

## 3. What you can do (preparer)

You are set up as a **preparer**, so you can work day-to-day tax workflow (not admin-only tools like Email Inbox or Staff Accounts).

### Main bar
- **Dashboard** — find clients / returns, filters, status
- **Bookkeeping** — monthly bookkeeping queue
- **New Intake** — walk-in / returning client worksheet

### Daily menu (most of your day)
- **Pickup Queue** — clients ready at the counter  
- **E-File Queue** — returns ready to transmit / batch  
- **Work Orders** — non-return walk-in requests  
- **Now Serving** — lobby take-a-number (also the **# Now Serving** button in the header). Call Next, send to the other window; panel stays open while you work  
- **Extension Queue** — extensions to batch / file  
- **Docs not scanned** — finish scans skipped at intake  

### On a return
- Open a client from the dashboard → documents, notes, missing docs, **Prep** workspace, status changes, scan/upload  

### Occasional (when needed)
- **Compliance Tracker** / **Filing Periods** — sales tax & license filings  
- Batches, source compare, etc. as your role allows  

**Admin-only (you will not see these):** Email Inbox, Email Campaigns, Staff Accounts, Audit log, Sender Rules, day reset on Now Serving, etc.

---

## 4. First-day checklist

- [ ] Open http://192.168.1.173:5000 and sign in  
- [ ] Change password from `temp@92`  
- [ ] Finish Welcome / orientation if shown  
- [ ] Find a known client on the Dashboard  
- [ ] Open **Daily → Now Serving** (or header button) and confirm the panel opens  
- [ ] Open **Daily → Pickup Queue** and **E-File Queue** once so you know where they live  
- [ ] Ask a teammate which status they use for “I’m working this return in Drake”  

---

## 5. Quick tips

- **Privacy** toggle in the header masks names when a client is at your desk.  
- **Text size** controls are next to Privacy.  
- **Now Serving** ticket numbers are plain numbers (1, 2, 3…). The lobby TV/kiosk announces “Now serving number …” in English and Spanish.  
- Stuck? Help icon → Guided Tour, or footer **Ops Runbook**, or ask Moises / admin.

---

## 6. For the admin (Moises)

Accounts already in TaxOps:

| Username | Display | Role | Notes |
|----------|---------|------|--------|
| `lucy` | Lucy | preparer | `must_change_password` should be on until first successful change |
| `sandra` | Sandra | preparer | same |

Temporary password for both (this handoff only): **`temp@92`**

After they change passwords, confirm they can open Dashboard + Now Serving. If login fails, reset password again from **Occasional → Staff Accounts** and re-enable “must change password.”
