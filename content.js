const ITERATION = 'Iteration 20.6.19';
console.log(`Initializing monitor — ${ITERATION}`);

// Improved auto-scroller: requires the page height to remain stable for a
// number of consecutive checks before resolving. This helps with slow or
// incremental lazy-loading where elements append after short delays.
const autoScrollToFullBottom = ({ step = 600, delay = 250, stableChecks = 4 } = {}) => new Promise(resolve => {
  let timer = null;
  let lastHeight = document.body.scrollHeight;
  let stableCount = 0;

  const scrollAndCheck = () => {
    try { window.scrollBy(0, step); } catch (e) { /* ignore */ }
    const currentHeight = document.body.scrollHeight;
    const atBottom = (window.innerHeight + window.scrollY) >= (currentHeight - 2);

    if (currentHeight === lastHeight) {
      stableCount += 1;
    } else {
      stableCount = 0;
      lastHeight = currentHeight;
    }

    if (atBottom && stableCount >= stableChecks) {
      clearInterval(timer);
      // allow micro-tasks to settle
      setTimeout(resolve, 0);
    }
  };

  // Run immediately to avoid waiting a full interval for first progress
  scrollAndCheck();
  timer = setInterval(scrollAndCheck, delay);
});

class ValueMonitor {
  constructor() {
    // config/state
    this.telegramToken = '';
    this.chatId = '';
    this.previousValues = null;
    this.checkInterval = null;
    this._dailyTimerId = null;
    this.isChecking = false;
    // identity/keys/timeouts
    this._instanceId = Math.random().toString(36).slice(2);
    this._dailyLockKey = 'dailyLock';
    this._dailyStatsKey = 'dailyStats';
    this._lastSuccessfulKey = 'lastSuccessfulDailyReport';
    this._dailyLockTimeoutMs = 2 * 60 * 1000;
    // new keys/guards
    this._dailyPlannedKey = this._dailyPlannedKey;
    this._lastDailySentKey = 'lastDailySentAt';
    this._dailyLockBaseKey = 'dailyLock';
    this._dailyLockHoldMs = 3 * 60 * 1000; // hold daily lock for 3 minutes

  // processing lock to avoid race between periodic checks and daily summary
  this._processingLockKey = 'processingLock';
  this._processingLockTimeoutMs = 2 * 60 * 1000; // 2 minutes
    this._dailyMaxPreSendRetries = 5;
    this._dailyPreSendBaseBackoffMs = 300;
    this._dailyScheduleJitterMs = 10 * 1000; // reduced jitter ±10s
    this._defaultFallbackHours = 48;
    this.notifySummaryMode = false;
    this._telegramMaxMessageChars = 4000;
    this._suspiciousDeltaLimit = 200;
  }

  // logging shorthands (preserve outputs)
  log(...a){ console.log(...a); }
  warn(...a){ console.warn(...a); }
  error(...a){ console.error(...a); }

  // period key uses user's dailyNotificationTime or 12:00 default
  async getCurrentPeriodKey() {
    const cfg = await new Promise(res => chrome.storage.sync.get(['dailyNotificationTime'], r =>
      res(r && r.dailyNotificationTime ? r.dailyNotificationTime : '12:00')));
    const [hourStr, minuteStr] = String(cfg).split(':');
    const hour = Number.isFinite(Number(hourStr)) ? Number(hourStr) : 12;
    const minute = Number.isFinite(Number(minuteStr)) ? Number(minuteStr) : 0;
    const now = new Date();
    const candidate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, minute, 0, 0);
    if (candidate > now) candidate.setDate(candidate.getDate() - 1);
    const pad = n => String(n).padStart(2,'0');
    const offset = -candidate.getTimezoneOffset();
    const sign = offset >= 0 ? '+' : '-';
    const offsetHours = pad(Math.floor(Math.abs(offset)/60));
    const offsetMins = pad(Math.abs(offset)%60);
    return `${candidate.getFullYear()}-${pad(candidate.getMonth()+1)}-${pad(candidate.getDate())}T${pad(candidate.getHours())}:${pad(candidate.getMinutes())}:00${sign}${offsetHours}:${offsetMins}`;
  }

  // split large telegram messages into parts keeping paragraphs
  _splitMessageIntoParts(message='', maxLen=this._telegramMaxMessageChars) {
    if (!message) return [];
    if (message.length <= maxLen) return [message];
    const parts=[]; const paragraphs = message.split('\n\n'); let current='';
    for (const p of paragraphs) {
      const chunk = (current ? '\n\n' : '') + p;
      if ((current + chunk).length > maxLen) {
        if (current) { parts.push(current); current = p; if (current.length > maxLen) { let s=0; while (s < current.length){ parts.push(current.slice(s, s+maxLen)); s+=maxLen;} current=''; } }
        else { let s=0; while (s < p.length){ parts.push(p.slice(s, s+maxLen)); s+=maxLen; } current=''; }
      } else current += chunk;
    }
    if (current) parts.push(current);
    return parts;
  }

  // Telegram send helpers with one retry
  async sendTelegramMessage(message, attempt=1) {
    if (!this.telegramToken || !this.chatId) { this.error('Missing Token or Chat ID'); return false; }
    const parts = this._splitMessageIntoParts(message, this._telegramMaxMessageChars);
    for (const part of parts) {
      const payload = { chat_id: this.chatId, text: part, parse_mode: 'HTML' };
      this.log('→ Telegram payload (part):', { len: part.length });
      try {
        const res = await fetch(`https://api.telegram.org/bot${this.telegramToken}/sendMessage`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
        const body = await res.json();
        if (!res.ok) {
          this.error('← Telegram API error:', body);
          if (attempt < 2) { this.log('Retrying Telegram send...'); await new Promise(r=>setTimeout(r,1000)); return this.sendTelegramMessage(message, attempt+1); }
          return false;
        }
        this.log('← Telegram API ok:', body);
      } catch (err) {
        this.error('Error sending message:', err);
        if (attempt < 2) { this.log('Retrying Telegram send...'); await new Promise(r=>setTimeout(r,1000)); return this.sendTelegramMessage(message, attempt+1); }
        return false;
      }
      await new Promise(r=>setTimeout(r,200));
    }
    return true;
  }

  async sendTelegramMessageWithPhoto(message, photoUrl) {
    if (!this.telegramToken || !this.chatId || !photoUrl) { this.log('Falling back to text message (missing token/chat/photo).'); return this.sendTelegramMessage(message); }
    try {
      this.log('Attempting to send photo:', { photoUrl, chatId: this.chatId });
      const imgRes = await fetch(photoUrl);
      if (!imgRes.ok) throw new Error(`Image download failed: ${imgRes.status}`);
      const blob = await imgRes.blob();
      const form = new FormData(); form.append('chat_id', this.chatId); form.append('caption', message); form.append('photo', blob, 'model_image.jpg');
      const res = await fetch(`https://api.telegram.org/bot${this.telegramToken}/sendPhoto`, { method:'POST', body: form });
      const result = await res.json();
      this.log('Telegram response:', result);
      if (!res.ok) throw new Error(`Telegram Error: ${res.status}`);
      return true;
    } catch (err) {
      this.error('Error sending photo:', err);
      return this.sendTelegramMessage(message);
    }
  }

  // scraping/parsing
  parseNumber(text){ if (!text) return 0; text = String(text).trim().toLowerCase(); if (text.includes('k')){ const base = parseFloat(text.replace('k','')); if (Number.isFinite(base)) return Math.round(base*1000); } const n = parseInt(text.replace(/[^\d]/g,''),10); return Number.isFinite(n)? n:0; }

  getCurrentValues() {
    try {
      const currentValues = { models: {}, points: 0, timestamp: Date.now() };
      try {
        const pointsContainer = document.querySelector('.mw-css-1541sxf');
        this.log('Found points container:', !!pointsContainer);
        if (pointsContainer) {
          const pts = pointsContainer.textContent.trim().match(/[\d,]+(\.\d+)?/);
          if (pts && pts[0]) { currentValues.points = parseFloat(pts[0].replace(/,/g,'')); this.log('Points found:', currentValues.points); }
        }
      } catch (e){ this.error('Error extracting points:', e); }
      const downloadElements = document.querySelectorAll('[data-trackid]');
      downloadElements.forEach(element => {
        const modelId = element.getAttribute('data-trackid');
        const modelTitle = element.querySelector('h3.translated-text');
        const name = modelTitle?.textContent.trim() || 'Model';
        const imageUrl = element.querySelector('img')?.getAttribute('src') || '';
        let permalink = null;
        const anchor = element.querySelector('a[href*="/models/"], a[href*="/model/"], a[href*="/models/"]');
        if (anchor?.href) permalink = anchor.href;
        const allMetrics = element.querySelectorAll('.mw-css-xlgty3 span');
        if (allMetrics.length >= 3) {
          const lastThree = Array.from(allMetrics).slice(-3);
          const boosts = this.parseNumber(lastThree[0]?.textContent || '0');
          const downloads = this.parseNumber(lastThree[1]?.textContent || '0');
          const prints = this.parseNumber(lastThree[2]?.textContent || '0');
          currentValues.models[modelId] = { id: modelId, permalink, name, boosts, downloads, prints, imageUrl };
          this.log(`Model "${name}":`, { id: modelId, boosts, downloads, prints, permalink });
        } else this.log(`Not enough metrics for ${name} (found ${allMetrics.length})`);
      });
      return currentValues;
    } catch (err) { this.error('Error extracting values:', err); return null; }
  }

  // reward math
  getRewardInterval(totalDownloads){ if (totalDownloads <= 50) return 10; if (totalDownloads <= 500) return 25; if (totalDownloads <= 1000) return 50; return 100; }
  nextRewardDownloads(totalDownloads){ const interval = this.getRewardInterval(totalDownloads); const mod = totalDownloads % interval; return (totalDownloads === 0 || mod === 0) ? totalDownloads + interval : totalDownloads + (interval - mod); }
  getRewardPointsForDownloads(thresholdDownloads){ if (thresholdDownloads <= 50) return 15; if (thresholdDownloads <= 500) return 12; if (thresholdDownloads <= 1000) return 20; return 30; }
  calculateDownloadsEquivalent(downloads, prints){ return Number(downloads||0) + (Number(prints||0) * 2); }
    getRewardCategory(downloads, prints) {
    const total = this.calculateDownloadsEquivalent(downloads, prints);
    if (total <= 49) return 1;
    if (total <= 499) return 2;
    if (total <= 999) return 3;
    return 4;
  }


  // storage lock helpers
  async acquireDailyLock(timeoutMs = this._dailyLockTimeoutMs) {
    // Use a per-day lock key to avoid cross-day collisions
    const today = new Date().toISOString().slice(0,10);
    const lockKey = `${this._dailyLockBaseKey}_${today}`;

    const now = Date.now();
    return new Promise(resolve => chrome.storage.local.get([lockKey], res => {
      const lock = res?.[this._dailyLockKey] || null;
      if (!lock || (now - lock.ts) > timeoutMs) {
        if (lock && (now - lock.ts) > timeoutMs) {
          chrome.storage.local.remove([lockKey], () => {
            const newLock = { ts: now, owner: this._instanceId };
            chrome.storage.local.set({ [lockKey]: newLock }, () => {
              chrome.storage.local.get([lockKey], r2 => {
                const confirmed = r2?.[this._dailyLockKey]?.owner === this._instanceId;
                this.log('acquireDailyLock (force unlock) result', { confirmed, owner: r2?.[this._dailyLockKey]?.owner, instance: this._instanceId });
                resolve(confirmed);
              });
            });
          });
        } else {
          const newLock = { ts: now, owner: this._instanceId };
          chrome.storage.local.set({ [lockKey]: newLock }, () => {
            chrome.storage.local.get([lockKey], r2 => {
              const confirmed = r2?.[this._dailyLockKey]?.owner === this._instanceId;
              this.log('acquireDailyLock result', { confirmed, owner: r2?.[this._dailyLockKey]?.owner, instance: this._instanceId });
              resolve(confirmed);
            });
          });
        }
      } else { this.log('acquireDailyLock failed, existing lock', lock); resolve(false); }
    }));
  }

  async releaseDailyLock() {
    return new Promise(resolve => chrome.storage.local.get([lockKey], res => {
      const lock = res?.[this._dailyLockKey] || null;
      if (lock && lock.owner === this._instanceId) {
        chrome.storage.local.remove([lockKey], () => { this.log('releaseDailyLock: released by', this._instanceId); resolve(true); });
      } else resolve(false);
    }));
  }

  // processing lock helpers (shorter timeout) to avoid race between periodic checks and daily summary
  async acquireProcessingLock(timeoutMs = this._processingLockTimeoutMs) {
    const now = Date.now();
    return new Promise(resolve => chrome.storage.local.get([this._processingLockKey], res => {
      const lock = res?.[this._processingLockKey] || null;
      if (!lock || (now - lock.ts) > timeoutMs) {
        const newLock = { ts: now, owner: this._instanceId };
        chrome.storage.local.set({ [this._processingLockKey]: newLock }, () => {
          chrome.storage.local.get([this._processingLockKey], r2 => {
            const confirmed = r2?.[this._processingLockKey]?.owner === this._instanceId;
            this.log('acquireProcessingLock result', { confirmed, owner: r2?.[this._processingLockKey]?.owner, instance: this._instanceId });
            resolve(confirmed);
          });
        });
      } else { this.log('acquireProcessingLock failed, existing lock', lock); resolve(false); }
    }));
  }

  async releaseProcessingLock() {
    return new Promise(resolve => chrome.storage.local.get([this._processingLockKey], res => {
      const lock = res?.[this._processingLockKey] || null;
      if (lock && lock.owner === this._instanceId) {
        chrome.storage.local.remove([this._processingLockKey], () => { this.log('releaseProcessingLock: released by', this._instanceId); resolve(true); });
      } else resolve(false);
    }));
  }

  // ---- BEGIN ADDITION: ReloadGuard helpers (insert into ValueMonitor class) ----

  // Count models within 2 downloads (downloads + 2*prints) of the next reward
  _countCloseToAward(models) {
    if (!models) return 0;
    let close = 0;
    for (const m of Object.values(models)) {
      const downloads = Number(m.downloads || 0);
      const prints = Number(m.prints || 0);
      const total = this.calculateDownloadsEquivalent(downloads, prints);
      const next = this.nextRewardDownloads(total);
      const remaining = Math.max(0, next - total);
      if (remaining <= 2) close++;
    }
    return close;
  }

  /*
    Detect incomplete load using 2-way check you specified.
    prevModels: object (previous snapshot.models)
    currModels: object (current snapshot.models)
    awardedCount: number (models that actually received an award in this run)
    Returns:
   { suspect: bool, details: { prevTotal, currTotal, prevClose, currClose,
   adjustedCurrClose, awardedCount, totalDrop, closeDrop } }
  */
  _detectIncompleteLoadChecks(prevModels = {}, currModels = {}, awardedCount = 0) {
    const prevTotal = Object.keys(prevModels || {}).length;
    const currTotal = Object.keys(currModels || {}).length;
    const prevClose = this._countCloseToAward(prevModels || {});
    const currClose = this._countCloseToAward(currModels || {});
    const adjustedCurrClose = currClose + (Number(awardedCount) || 0);
    const totalDrop = prevTotal - currTotal;
    const closeDrop = prevClose - adjustedCurrClose;

    const suspect = (totalDrop >= 4) && (closeDrop >= 2);
  
   return { suspect, details: { prevTotal, currTotal, prevClose,
  currClose, adjustedCurrClose, awardedCount, totalDrop, closeDrop } };
  }

  // Soft re-scrape: scroll to bottom (auto-scroll repeated short steps) then re-run the DOM scrape method
  async _rescrapeSoft({ step = 600, delay = 250, stableChecks = 3 } = {}) {
    try {
      // auto-scroll to bottom and wait for the DOM to settle
      let lastHeight = document.body.scrollHeight;
      let stable = 0;
      while (true) {
        window.scrollBy(0, step);
        await new Promise(r => setTimeout(r, delay));
        const h = document.body.scrollHeight;
        const atBottom = (window.innerHeight + window.scrollY) >= (h - 2);
        if (h === lastHeight) stable++; else { stable = 0; lastHeight = h; }
        if (atBottom && stable >= stableChecks) break;
      }
      // small settle delay
      await new Promise(r => setTimeout(r, 300));
      // use your existing scrape function (e.g., getCurrentValues() or _scrapeData())
      const newValues = this.getCurrentValues ? this.getCurrentValues() : (await this._scrapeData());
      return newValues;
    } catch (err) {
      console.warn('[ReloadGuard] _rescrapeSoft failed', err);
      return null;
    }
  }

  // Per-day reload cap & cooldown
  async _shouldReloadToday() {
    const now = Date.now();
    const today = new Date().toISOString().slice(0,10);
    return new Promise(res => {
      chrome.storage.local.get(['reloadCountDate','reloadCount','lastReloadAt'], r => {
        const storedDate = r.reloadCountDate;
        const reloadCount = r.reloadCount || 0;
        const lastReloadAt = r.lastReloadAt || 0;
        // reset if different day
        if (storedDate !== today) {
          chrome.storage.local.set({ reloadCountDate: today, reloadCount: 0 }, () => res(true));
          return;
        }
        const cooldownMs = 60 * 1000; // 1 minute cooldown
        const cap = 3; // 3 reloads per day
        if (reloadCount >= cap) return res(false);
        if ((now - lastReloadAt) < cooldownMs) return res(false);
        return res(true);
      });
    });
  }

  async _incrementReloadCount() {
    const today = new Date().toISOString().slice(0,10);
    return new Promise(res => {
      chrome.storage.local.get(['reloadCountDate','reloadCount'], r => {
        const storedDate = r.reloadCountDate;
        let reloadCount = r.reloadCount || 0;
        if (storedDate !== today) {
          reloadCount = 1;
  
         chrome.storage.local.set({ reloadCountDate: today, reloadCount,
  lastReloadAt: Date.now() }, () => res({ reloadCount, today }));
        } else {
          reloadCount += 1;
          chrome.storage.local.set({ reloadCount, lastReloadAt: Date.now() }, () => res({ reloadCount, today }));
        }
      });
    });
  }

  // ---- END ADDITION ----

  // pre-send check to avoid duplicate daily sends
  async preSendCheckAndMaybeWait(startTime) {
    for (let attempt = 0; attempt < this._dailyMaxPreSendRetries; attempt++) {
      const latest = await new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r => res(r?.[this._dailyStatsKey] || null)));
      if (latest && latest.timestamp >= startTime) { this.log('preSendCheck: found newer dailyStats, aborting send', { latestTs: new Date(latest.timestamp).toISOString(), startTime: new Date(startTime).toISOString() }); return false; }
      const backoff = this._dailyPreSendBaseBackoffMs + Math.floor(Math.random()*700);
      await new Promise(r => setTimeout(r, backoff));
    }
    return true;
  }

  // side-effect-free computation of rewards since baseline
  async computeRewardsSinceBaseline() {
    await autoScrollToFullBottom();
    const currentValues = this.getCurrentValues();
    if (!currentValues) {
      this.error('Unable to get current values for compute');
      return { rewardPointsTotal: 0, dailyDownloads: 0, dailyPrints: 0, dailyBoosts: 0, points: 0, pointsGained: 0, modelChanges: {}, rewardsEarned: [] /* add other fields as needed */ };
    }

    const previousDayRaw = await new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r => res(r?.[this._dailyStatsKey] || null)));
    const maxStaleMs = await new Promise(res => chrome.storage.sync.get(['dailyFallbackMaxAgeMs'], cfg => {
      const cfgVal = cfg?.dailyFallbackMaxAgeMs; res(Number.isFinite(cfgVal) ? cfgVal : (this._defaultFallbackHours * 60 * 60 * 1000));
    }));

    let previousDay = null;
    if (previousDayRaw) {
      const ageMs = Date.now() - previousDayRaw.timestamp;
      if (ageMs <= 24 * 60 * 60 * 1000) previousDay = previousDayRaw;
      else if (ageMs <= maxStaleMs) previousDay = previousDayRaw, this.warn('Using stale baseline for compute');
      else this.log('Baseline too old, treating as missing');
    }

    if (!previousDay) {
      // No baseline: return empty (don't update storage here)
      return { dailyDownloads: 0, dailyPrints: 0, dailyBoosts: 0, points: currentValues.points, pointsGained: 0, modelChanges: {}, rewardsEarned: [], rewardPointsTotal: 0 };
    }

    // Compute modelChanges, rewards
    const modelChanges = {};
    for (const [id, current] of Object.entries(currentValues.models)) {
      let previous = previousDay?.models?.[id] || null;
      if (!previous && current.permalink) previous = Object.values(previousDay.models || {}).find(m => m?.permalink === current.permalink) || null;
      if (!previous && current.name) { const norm = current.name.trim().toLowerCase(); previous = Object.values(previousDay.models || {}).find(m => m?.name?.trim().toLowerCase() === norm) || null; }
      if (!previous) continue;

      const prevDownloads = Number(previous.downloads || 0), prevPrints = Number(previous.prints || 0), currDownloads = Number(current.downloads || 0), currPrints = Number(current.prints || 0);
      const prevBoosts = Number(previous.boosts || 0), currBoosts = Number(current.boosts || 0);
      let downloadsGained = currDownloads - prevDownloads, printsGained = currPrints - prevPrints, boostsGained = currBoosts - prevBoosts;

      if (downloadsGained <= 0 && printsGained <= 0 && boostsGained <= 0) continue;
      if (downloadsGained > this._suspiciousDeltaLimit || printsGained > this._suspiciousDeltaLimit) continue;

      modelChanges[id] = { id, name: current.name, downloadsGained, printsGained, boostsGained, previousDownloads: prevDownloads, previousPrints: prevPrints, currentDownloads: currDownloads, currentPrints: currPrints, permalink: current.permalink || previous?.permalink || null };
    }

    const dailyDownloads = Object.values(modelChanges).reduce((s, m) => s + m.downloadsGained, 0);
    const dailyPrints = Object.values(modelChanges).reduce((s, m) => s + m.printsGained, 0);
    const dailyBoosts = Object.values(modelChanges).reduce((s, m) => s + (m.boostsGained || 0), 0);

    const rewardsEarned = [];
    let rewardPointsTotal = 0;
    for (const m of Object.values(modelChanges)) {
      const prevDownloadsTotal = this.calculateDownloadsEquivalent(m.previousDownloads, m.previousPrints);
      const currentDownloadsTotal = this.calculateDownloadsEquivalent(m.currentDownloads, m.currentPrints);
      let cursor = prevDownloadsTotal; const thresholdsHit = []; const maxThresholdsPerModel = 200; let thresholdsCount = 0;
      while (cursor < currentDownloadsTotal && thresholdsCount < maxThresholdsPerModel) {
        const interval = this.getRewardInterval(cursor);
        const mod = cursor % interval;
        const nextThreshold = (cursor === 0 || mod === 0) ? cursor + interval : cursor + (interval - mod);
        if (nextThreshold <= currentDownloadsTotal) {
          const rewardPoints = this.getRewardPointsForDownloads(nextThreshold);
          thresholdsHit.push({ threshold: nextThreshold, rewardPoints });
          rewardPointsTotal += rewardPoints;
          cursor = nextThreshold; thresholdsCount++;
        } else break;
      }
      if (thresholdsHit.length) rewardsEarned.push({ id: m.id, name: m.name, thresholds: thresholdsHit.map(t => t.threshold), rewardPointsTotalForModel: thresholdsHit.reduce((s, t) => s + t.rewardPoints, 0) });
    }

    return { dailyDownloads, dailyPrints, dailyBoosts, points: currentValues.points, pointsGained: currentValues.points - previousDay.points, rewardsEarned, rewardPointsTotal, modelChanges, from: new Date(previousDay.timestamp).toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }), to: new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }) };
  }

  // robust daily summary computation and storage
  async getDailySummary() {
    await autoScrollToFullBottom();
    const summary = await this.computeRewardsSinceBaseline();
    const currentValues = this.getCurrentValues();
    const periodKey = await this.getCurrentPeriodKey();
    chrome.storage.local.set({ [this._dailyStatsKey]: { models: currentValues.models, points: currentValues.points, timestamp: Date.now(), owner: this._instanceId, periodKey } }, () => {
      this.log('getDailySummary: updated dailyStats ts=', new Date().toISOString(), 'modelsCount=', Object.keys(currentValues.models || {}).length, 'owner=', this._instanceId, 'periodKey', periodKey);
    });
    return summary;
  }

  // schedule daily report with locking/claiming (robust: persist planned time and detect missed after reload)
  async scheduleDailyNotification() {
    if (this._dailyTimerId) { clearTimeout(this._dailyTimerId); this._dailyTimerId = null; }
    // Guard: if a planned daily run is already persisted for a future time, skip scheduling a duplicate
    try {
      const planned = await new Promise(res => chrome.storage.local.get([this._dailyPlannedKey], r => res(r?.[this._dailyPlannedKey] || null)));
      if (planned && planned > Date.now()) {
        this.log('scheduleDailyNotification: a planned daily run already exists for a future time; skipping duplicate schedule.', new Date(planned).toLocaleString());
        return;
      }
    } catch (e) {
      // ignore storage read errors and continue scheduling
    }

    chrome.storage.sync.get(['dailyReport','dailyNotificationTime'], (config) => {
      const dailyReport = config.dailyReport || 'yes';
      if (dailyReport === 'no') { this.log('Daily report disabled'); return; }
      const dailyTime = config.dailyNotificationTime || '12:00';
      const [hour, minute] = dailyTime.split(':').map(Number);

      // compute next notification at the configured hour/minute (local)
      const now = new Date();
      let nextNotification = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hour, minute, 0, 0);
      if (nextNotification.getTime() <= now.getTime()) nextNotification.setDate(nextNotification.getDate() + 1);

      // small jitter to avoid exact collisions
      const jitter = Math.floor((Math.random() * 2 - 1) * this._dailyScheduleJitterMs);
      const delay = Math.max(0, nextNotification.getTime() - now.getTime() + jitter);

      // persist the planned daily run so a reload can detect a missed run
      try {
        chrome.storage.local.set({ [this._dailyPlannedKey]: nextNotification.getTime() });
      } catch (e) {
        // ignore storage errors
      }

      this.log(`Daily report scheduled for: ${new Date(nextNotification.getTime()).toLocaleString()}. Delay: ${delay}ms; jitterMs=${jitter}`);
      this._dailyTimerId = setTimeout(async () => {
        const startTime = Date.now();
        this.log(`scheduleDailyNotification: firing attempt at ${new Date().toISOString()}`);

        // Deduplication: if a daily send recently occurred, skip this run
        try {
          const lastSent = await new Promise(res => chrome.storage.local.get([this._lastDailySentKey], r => res(r?.[this._lastDailySentKey] || null)));
          const MIN_MS_BETWEEN_DAILIES = 5 * 60 * 1000; // 5 minutes
          if (lastSent && (Date.now() - lastSent) < MIN_MS_BETWEEN_DAILIES) {
            this.log('scheduleDailyNotification: skipping because a daily summary was sent recently', new Date(lastSent).toISOString());
            // clear persisted planned marker and schedule tomorrow
            try { chrome.storage.local.remove(this._dailyPlannedKey); } catch(e){}
            this._dailyTimerId = null;
            this.scheduleDailyNotification();
            return;
          }
        } catch(e) { /* ignore storage errors */ }


        // When the daily runs, remove the persisted planned marker so other instances know it's handled
        try { chrome.storage.local.remove([this._dailyPlannedKey]); } catch (e) {}

        // Wait for any in-progress periodic processing to finish by checking the processing lock.
        // Retry up to MAX_WAIT_ATTEMPTS with small randomized backoff.
        const MAX_WAIT_ATTEMPTS = 10;
        let processingBusy = false;
        for (let i = 0; i < MAX_WAIT_ATTEMPTS; i++) {
          const lock = await new Promise(res => chrome.storage.local.get([this._processingLockKey], r => res(r?.[this._processingLockKey] || null)));
          if (!lock) { processingBusy = false; break; }
          processingBusy = true;
          const waitMs = 100 + Math.floor(Math.random() * 400);
          this.log(`scheduleDailyNotification: waiting for processing lock to clear (attempt ${i + 1})`);
          await new Promise(r => setTimeout(r, waitMs));
        }

        if (processingBusy) {
          // ---- BEGIN REPLACEMENT: robust multi-retry loop for same-day summary ----
          this.log('Daily summary collided with periodic check; entering retry loop.');

          const MAX_DAILY_RETRY_ATTEMPTS = 6;        // ~12 minutes total
          const RETRY_INTERVAL_MS = 2 * 60 * 1000;   // 2 minutes between attempts
          let attempt = 1;

          const trySend = async () => {
            this.log(`Daily summary retry attempt ${attempt}...`);

            const lock = await new Promise(res =>
              chrome.storage.local.get([this._processingLockKey],
                r => res(r?.[this._processingLockKey] || null))
            );

            if (!lock) {
              this.log('Processing lock cleared; sending daily summary now.');
              try {
                await this._compileAndSendDailySummary();
                // mark last sent timestamp
                try { chrome.storage.local.set({ [this._lastDailySentKey]: Date.now() }); } catch(e){}
              } catch (err) {
                this.error('Retry daily summary error:', err);
              } finally {
                // release per-day daily lock if present for today
                try { const today = new Date().toISOString().slice(0,10); const lockKey = `${this._dailyLockBaseKey}_${today}`; chrome.storage.local.remove([lockKey]); } catch(e){}
                this._dailyTimerId = null;
                this.scheduleDailyNotification(); // schedule tomorrow
              }
              return;
            }

            if (attempt < MAX_DAILY_RETRY_ATTEMPTS) {
              attempt++;
              setTimeout(trySend, RETRY_INTERVAL_MS);
            } else {
              this.warn('Daily summary still blocked after retries; skipping for today.');
              chrome.storage.local.remove(this._dailyLockKey);
              this._dailyTimerId = null;
              this.scheduleDailyNotification(); // schedule tomorrow
            }
          };

          setTimeout(trySend, RETRY_INTERVAL_MS);
          return;
          // ---- END REPLACEMENT ----
        }

        // Acquire an explicit daily lock to prevent concurrent daily runs across instances
        const acquired = await new Promise(res => chrome.storage.local.get([this._dailyLockKey], r => {
          const existing = r?.[this._dailyLockKey];
          if (existing) return res(false);
          chrome.storage.local.set({ [this._dailyLockKey]: Date.now() }, () => res(true));
        }));

        if (!acquired) {
          this.log('scheduleDailyNotification: could not acquire daily lock; will retry in 60 seconds');
          setTimeout(() => this.scheduleDailyNotification(), 60 * 1000);
          return;
        }

        try {
          // Also set a processing lock here so the periodic job yields to the daily summary.
          await new Promise(res => chrome.storage.local.set({ [this._processingLockKey]: Date.now() }, res));
          try {
            await this._compileAndSendDailySummary();
          } finally {
            // release processing lock
            chrome.storage.local.remove(this._processingLockKey);
          }
        } catch (err) {
          this.log('scheduleDailyNotification: error during daily summary', err);
        } finally {
          // release daily lock and schedule next day's run
          chrome.storage.local.remove(this._dailyLockKey);
          this._dailyTimerId = null;
          // schedule next day's notification
          this.scheduleDailyNotification();
        }
      }, delay);
    });
  }


  // main periodic check (per-model messages or summary)
  async checkAndNotify() {
    // Acquire a short-lived processing lock to prevent the daily summary from
    // racing with periodic checks. If the lock cannot be acquired after a few
    // attempts, skip this check to avoid overlapping work.
    const MAX_LOCK_ATTEMPTS = 3;
    let lockAcquired = false;
    for (let attempt = 1; attempt <= MAX_LOCK_ATTEMPTS; attempt++) {
      lockAcquired = await this.acquireProcessingLock();
      if (lockAcquired) break;
      const backoff = 100 + Math.floor(Math.random() * 200);
      this.log(`checkAndNotify: processing lock busy, retrying in ${backoff}ms (attempt ${attempt})`);
      await new Promise(r => setTimeout(r, backoff));
    }
    if (!lockAcquired) { this.log('Check skipped: processing lock could not be acquired.'); return; }
    if (this.isChecking) { this.log('Check already in progress, skipping...'); await this.releaseProcessingLock(); return; }
    this.log(`checkAndNotify start — ${ITERATION}`); this.isChecking = true;
    try {
      this.log('Starting change check...');
      let anyNotification = false;
      let currentValues = this.getCurrentValues();
      if (!currentValues) { this.log('No current values found'); await this.savePreviousValues({}); return; }
      if (!this.previousValues) await this.loadPreviousValues();
      if (!this.previousValues) { this.log('First run or no previous values, saving initial values'); this.previousValues = currentValues; await this.savePreviousValues(currentValues); return; }
      if (this.previousValues && !this.previousValues.models) { this.previousValues.models = {}; await this.savePreviousValues(this.previousValues); }
      if (currentValues.points > (this.previousValues.points || 0)) this.log('Global account points increased, ignoring for per-model-only Telegram notifications.');

      // This helper function computes the differences and rewards.
      const _rebuildModelSummaries = (prev, curr) => {
        const summaries = {};
        for (const [id, current] of Object.entries(curr.models || {})) {
          const previous = prev.models ? prev.models[id] : undefined;
          if (!previous || !current) continue;
          
          const previousDownloadsRaw = Number(previous.downloads) || 0;
          const previousPrints = Number(previous.prints) || 0;
          const previousBoosts = Number(previous.boosts) || 0;
          const previousDownloadsTotal = this.calculateDownloadsEquivalent(previousDownloadsRaw, previousPrints);

          const currentDownloadsRaw = Number(current.downloads) || 0;
          const currentPrints = Number(current.prints) || 0;
          const currentBoosts = Number(current.boosts) || 0;

          let currentDownloadsTotal = 0;
          try {
            currentDownloadsTotal = this.calculateDownloadsEquivalent(currentDownloadsRaw, currentPrints);
          } catch (err) {
            this.warn('_rebuildModelSummaries: failed to compute currentDownloadsTotal for', id, err);
            currentDownloadsTotal = currentDownloadsRaw + currentPrints * 2; // safe fallback
          }

          const downloadsDeltaRaw = currentDownloadsRaw - previousDownloadsRaw;
          const printsDelta = currentPrints - previousPrints;
          const boostsDelta = currentBoosts - previousBoosts;
          const downloadsDeltaEquivalent = downloadsDeltaRaw + (printsDelta * 2);
          
          const hasActivity = (downloadsDeltaRaw !== 0) || (printsDelta !== 0) || (boostsDelta > 0);
          
          const modelSummary = {
            id,
            name: current.name,
            imageUrl: current.imageUrl,
            downloadsDeltaRaw,
            printsDelta,
            boostsDelta,
            previousDownloadsTotal,
            currentDownloadsTotal,
            downloadsDeltaEquivalent,
            rewards: []
          };

          if (currentDownloadsTotal > previousDownloadsTotal) {
            let cursor = previousDownloadsTotal, maxRewardsToReport = 50, rewardsFound = 0;
            while (cursor < currentDownloadsTotal && rewardsFound < maxRewardsToReport) {
              const interval = this.getRewardInterval(cursor), mod = cursor % interval;
              const nextThreshold = (cursor === 0 || mod === 0) ? cursor + interval : cursor + (interval - mod);
              if (nextThreshold <= currentDownloadsTotal) {
                const rewardPoints = this.getRewardPointsForDownloads(nextThreshold);
                modelSummary.rewards.push({ thresholdDownloads: nextThreshold, points: rewardPoints });
                cursor = nextThreshold;
                rewardsFound++;
              } else {
                break;
              }
            }
          }
          if (hasActivity || modelSummary.rewards.length > 0) {
            summaries[id] = modelSummary;
          }
        }
        return summaries;
      };

      let modelSummaries = _rebuildModelSummaries(this.previousValues, currentValues);

      // ---- BEGIN INSERT: call reload-guard check before sending ----

      // Ensure we have a previous snapshot (use the persisted previousValues you already store)
      const prevModelsSnapshot = this.previousValues?.models || {};

      const awardedModelsThisRun = Object.values(modelSummaries || {}).filter(ms => (ms.rewards && ms.rewards.length > 0)).length;

      // run detection
      const detection = this._detectIncompleteLoadChecks(prevModelsSnapshot, currentValues.models || {}, awardedModelsThisRun);
      this.log('ReloadGuard initial detection:', detection);

      let warningPrefix = '';
      let diagnosticText = '';

      if (detection.suspect) {
        // 1) Try soft re-scrape
        const soft = await this._rescrapeSoft();
        if (soft) {
          const newDetection = this._detectIncompleteLoadChecks(prevModelsSnapshot, soft.models || {}, awardedModelsThisRun);
          this.log('ReloadGuard after soft re-scrape:', newDetection);
          if (!newDetection.suspect) {
            // use the rescrape values for send
            currentValues = soft;
            // recompute modelSummaries vs previousValues
            modelSummaries = _rebuildModelSummaries(this.previousValues, currentValues);
            this.log('ReloadGuard: soft re-scrape successful. Using new data.');
          } else {
            // still suspect after soft rescrape
            const canReload = await this._shouldReloadToday();
            if (canReload) {
              await this._incrementReloadCount();
              this.log('ReloadGuard: reloading page to recover full data...');
              window.location.reload();
              // IMPORTANT: abort this cycle so we don't send a potentially bad update
              return;
            } else {
              // cannot reload (cap/cooldown) — send with warning and diagnostics
              warningPrefix = '⚠️ Data may be inaccurate due to incomplete page loading.\n\n';
              const d = detection.details;
              diagnosticText = `Diagnostics: prevTotal=${d.prevTotal}, currTotal=${d.currTotal}, totalDrop=${d.totalDrop}; prevClose=${d.prevClose}, currClose=${d.currClose}, adjustedCurrClose=${d.adjustedCurrClose}, closeDrop=${d.closeDrop}; awardedThisRun=${d.awardedCount}\n\n`;
            }
          }
        } else {
          // rescrape failed; same process: reload if allowed, else warn
          const canReload = await this._shouldReloadToday();
          if (canReload) {
            await this._incrementReloadCount();
            window.location.reload();
            return;
          } else {
            warningPrefix = '⚠️ Data may be inaccurate due to incomplete page loading.\n\n';
            const d = detection.details;
            diagnosticText = `Diagnostics: prevTotal=${d.prevTotal}, currTotal=${d.currTotal}, totalDrop=${d.totalDrop}; prevClose=${d.prevClose}, currClose=${d.currClose}, adjustedCurrClose=${d.adjustedCurrClose}, closeDrop=${d.closeDrop}; awardedThisRun=${d.awardedCount}\n\n`;
          }
        }
      }
      
      // ---- END INSERT ----


      const modelsActivity = [];
      const modelUpdateCount = Object.keys(modelSummaries).length;

      for (const [id, modelSummary] of Object.entries(modelSummaries)) {
        const current = currentValues.models[id];
        const { boostsDelta, downloadsDeltaRaw, printsDelta, rewards } = modelSummary;
        
        const boostOnly = (boostsDelta > 0) && (downloadsDeltaRaw === 0 && printsDelta === 0 && rewards.length === 0);
        if (!this.notifySummaryMode) {
          if (boostOnly) {
            const lines = [];
            lines.push(`⚡ Boost Update for: ${current.name}`, '', `⚡ Boosts: +${boostsDelta} (now ${current.boosts})`);
            let message = lines.join('\n');
            message = warningPrefix + diagnosticText + message; // Prepend warning if any
            warningPrefix = ''; diagnosticText = ''; // Clear after first use
            this.log('MESSAGE-BRANCH', { iteration: ITERATION, name: current.name, branch: 'boost-only', boostsDelta, rewardsFound: rewards.length });
            this.log(`Sending boost-only message for ${current.name}`);
            const sent = await this.sendTelegramMessageWithPhoto(message, modelSummary.imageUrl);
            anyNotification = true; continue;
          }

          const hasActivity2 = (downloadsDeltaRaw !== 0) || (printsDelta !== 0) || (rewards.length > 0) || (boostsDelta > 0);
          if (hasActivity2) {
            this.log('MESSAGE-BRANCH', { iteration: ITERATION, name: current.name, branch: 'milestone', downloadsDeltaEquivalent: modelSummary.downloadsDeltaEquivalent, boostsDelta, rewardsFound: rewards.length });
            const lines = []; const equivalentTotal = modelSummary.currentDownloadsTotal;
            lines.push(`📦 Update for: ${current.name}`, '', `${modelSummary.downloadsDeltaEquivalent > 0 ? '+' : ''}${modelSummary.downloadsDeltaEquivalent} Downloads (total ${equivalentTotal})`, '');
            if (rewards.length > 0) { rewards.forEach(r => lines.push(`🎁 Reward Earned! +${r.points} points at ${r.thresholdDownloads} downloads`)); lines.push(''); }
            const nextThresholdAfterCurrent = this.nextRewardDownloads(equivalentTotal);
            const downloadsUntilNext = Math.max(0, nextThresholdAfterCurrent - equivalentTotal);
            lines.push(`🎯 Next Reward: ${downloadsUntilNext} more downloads (${nextThresholdAfterCurrent} total)`, '', `🔁 Reward Interval: every ${this.getRewardInterval(equivalentTotal)} downloads`);
            if (boostsDelta > 0) lines.push('', `⚡ Boosts: +${boostsDelta} (now ${current.boosts})`);
            let warning = '';
            if (Math.abs(downloadsDeltaRaw) > this._suspiciousDeltaLimit || Math.abs(printsDelta) > this._suspiciousDeltaLimit) {
              warning = "\n\n⚠️ The number of downloads or prints during this period is very high. This could be because your model is very popular (good job!). Or it could be an error. You may want to shorten the refresh interval.";
            }
            let message = lines.join('\n') + warning;
            message = warningPrefix + diagnosticText + message; // Prepend warning if any
            warningPrefix = ''; diagnosticText = ''; // Clear after first use
            this.log(`Sending milestone message for ${current.name}`);
            const sent = await this.sendTelegramMessageWithPhoto(message, modelSummary.imageUrl);
            anyNotification = true;
          }
        } else {
          modelsActivity.push({ id, name: current.name, downloadsDeltaEquivalent: modelSummary.downloadsDeltaEquivalent, currentDownloadsTotal: modelSummary.currentDownloadsTotal, rewardPointsForThisModel: rewards.reduce((s,r)=>s+r.points,0), boostsDelta });
        }
      }

      // dynamic summary mode switch
      let forceSummaryMode = false;
      const SUMMARY_MODE_THRESHOLD = 15;
      if (!this.notifySummaryMode && modelUpdateCount >= SUMMARY_MODE_THRESHOLD) { forceSummaryMode = true; this.log(`Switching to summary mode for this check due to ${modelUpdateCount} updates.`); }
      const useSummaryMode = this.notifySummaryMode || forceSummaryMode;

      if (useSummaryMode) {
        if (forceSummaryMode) await this.sendTelegramMessage("Switching to summary mode due to the high number of updates this period. This ensures Telegram limits are not reached.");
        if (modelsActivity.length === 0) {
          await this.sendTelegramMessage(warningPrefix + diagnosticText + "No new prints or downloads found."); anyNotification = true;
          const prevString = JSON.stringify(this.previousValues||{}), currString = JSON.stringify(currentValues||{});
          if (prevString !== currString) { this.previousValues = currentValues; await this.savePreviousValues(currentValues); }
          this.isChecking = false; return;
        }

        const totalEquivalent = modelsActivity.reduce((s,m)=>s + (m.downloadsDeltaEquivalent||0),0);
        const rewardPointsThisRun = modelsActivity.reduce((s,m)=>s + (m.rewardPointsForThisModel||0),0);

        const computed = await this.computeRewardsSinceBaseline();
        const rewardsToday = computed.rewardPointsTotal;

        const fromTs = new Date(this.previousValues.timestamp).toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }), toTs = new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' });
        const headerLines = [`📊 Summary (${fromTs} - ${toTs}):`, '', `Downloads this period: ${totalEquivalent} (downloads + 2X prints)`, '', 'Model updates:', ''];
        const maxModelsInMessage = 200;
        const list = modelsActivity.slice(0, maxModelsInMessage);
        const modelLines=[]; let anyLargeDelta=false;
        list.forEach((m,i) => {
          const downloadsDelta = m.downloadsDeltaEquivalent || 0, total = m.currentDownloadsTotal || 0, interval = m.rewardInterval || this.getRewardInterval(total), nextThreshold = this.nextRewardDownloads(total), remaining = Math.max(0, nextThreshold - total), ptsEarned = m.rewardPointsForThisModel || 0;
          let line = `${i+1}. ${m.name}: +${downloadsDelta} (total ${total})`; if (ptsEarned>0) line += `  🎉 +${ptsEarned} pts`; line += ` (needs ${remaining} for next 🎁, interval ${interval})`;
          if (Math.abs(downloadsDelta) > this._suspiciousDeltaLimit) { line += `\n⚠️ The number of downloads during this period is very high. This could be because your model is very popular (good job!). Or it could be an error. You may want to shorten the refresh interval.`; anyLargeDelta=true; }
          if ((m.boostsDelta || 0) > 0) {
            line += `
⚡ Boosts: +${m.boostsDelta}`;
          }

          modelLines.push(line);
        });
        const spacedModels = modelLines.join('\n\n');
		// Count models within 2 downloads (downloads + 2×prints) of next reward across all models on the page
		let closeToGiftCount = 0;
		const allModels = Object.values(currentValues?.models || {});
		for (const cm of allModels) {
		  const downloads = Number(cm.downloads || 0);
		  const prints = Number(cm.prints || 0);
		  const total = this.calculateDownloadsEquivalent(downloads, prints); // downloads + 2*prints
		  const next = this.nextRewardDownloads(total);
		  const remaining = Math.max(0, next - total);
		  if (remaining <= 2) closeToGiftCount++;
		}
		const footerLines = [
		  '',
		  `Rewards this period: ${rewardPointsThisRun} pts`,
		  `Rewards today: ${rewardsToday} pts`,
		  `Models close to 🎁: ${closeToGiftCount}`
		];

        const message = warningPrefix + diagnosticText + headerLines.join('\n') + '\n' + spacedModels + '\n' + footerLines.join('\n');
        this.log('Aggregated summary message length:', message.length);
        const sent = await this.sendTelegramMessage(message); if (sent) anyNotification = true;
      } else {
        // per-model logic already executed inside loop
      }

      const prevString = JSON.stringify(this.previousValues || {}), currString = JSON.stringify(currentValues || {});
      if (prevString !== currString) { this.previousValues = currentValues; await this.savePreviousValues(currentValues); } else this.log('No changes detected, skipping savePreviousValues to reduce storage writes.');
      if (!anyNotification && !useSummaryMode) { const heartbeatMsg = 'No new prints or downloads found.'; this.log(heartbeatMsg); await this.sendTelegramMessage(warningPrefix + diagnosticText + heartbeatMsg); }
    } catch (err) { this.error('Error during check:', err); }
    finally { this.isChecking = false; try { await this.releaseProcessingLock(); } catch (e) { this.warn('Failed to release processing lock', e); } }
  }

  // previousValues persistence
  async loadPreviousValues(){ return new Promise(resolve => chrome.storage.local.get(['previousValues'], result => { if (result?.previousValues) { this.log('Previous values loaded:', result.previousValues); this.previousValues = result.previousValues; } resolve(); })); }
  async savePreviousValues(values){ return new Promise(resolve => chrome.storage.local.set({ previousValues: values }, () => { this.log('Values saved to storage'); resolve(); })); }

  // lifecycle
  async start() {
    this.log('Starting monitor...');
    if (this.checkInterval) { this.log('Monitor already running, skipping duplicate start.'); return; }
    chrome.storage.sync.get(['telegramToken','chatId','refreshInterval','dailyReport','dailyNotificationTime','notifySummaryMode'], async (config) => {
      if (!config || !config.telegramToken || !config.chatId) { this.error('Missing Telegram configuration'); return; }
      this.telegramToken = config.telegramToken; this.chatId = config.chatId; this.notifySummaryMode = !!config.notifySummaryMode;
      this._dailyNotificationTime = config.dailyNotificationTime || '12:00';
      const refreshInterval = config.refreshInterval || 900000;
      this.log(`Configured refresh interval: ${refreshInterval}ms`); this.log(`Notify summary mode: ${this.notifySummaryMode}`);
      let intervalToUse = refreshInterval; const ONE_HOUR = 60*60*1000; const COMPENSATION_MS = 60*1000;
      if (refreshInterval > ONE_HOUR) { intervalToUse = Math.max(0, refreshInterval - COMPENSATION_MS); this.log(`Interval adjusted for overhead: using ${intervalToUse}ms instead of configured ${refreshInterval}ms`); }
      else this.log(`Interval not adjusted (configured <= 1 hour): using ${intervalToUse}ms`);
      await autoScrollToFullBottom();
      await this.loadPreviousValues();
      await this.checkAndNotify();
      if (this.checkInterval) { clearInterval(this.checkInterval); this.checkInterval = null; }

      const STORAGE_KEY = 'monitorNextScheduledTime';
      // Use chrome.storage.local for persistence across browser restarts
      const stored = await new Promise(res => chrome.storage.local.get([STORAGE_KEY], r => res(r)));
      let nextScheduled = stored ? stored[STORAGE_KEY] : null;

      if (!nextScheduled || nextScheduled < Date.now()) {
        nextScheduled = Date.now() + intervalToUse;
        this.log('Initializing schedule. First run at:', new Date(nextScheduled).toLocaleString());
        chrome.storage.local.set({ [STORAGE_KEY]: nextScheduled });
      }

      const scheduleNext = () => {
        const now = Date.now();

        if (now > nextScheduled) {
          this.warn(`Missed scheduled time by ${Math.round((now - nextScheduled)/1000)}s. Running now.`);
          while (nextScheduled < now) {
            nextScheduled += intervalToUse;
          }
          chrome.storage.local.set({ [STORAGE_KEY]: nextScheduled });
        }

        const delay = Math.max(0, nextScheduled - now);
        this.log(`Next check scheduled for ${new Date(nextScheduled).toLocaleString()} (in ${Math.round(delay/1000)}s)`);

        this.checkInterval = setTimeout(async () => {
          try {
            this.log('Scrolling before refresh...');
            await autoScrollToFullBottom();
            this.log('Refreshing page...');
          } catch (err) {
            this.error('Error during pre-refresh tasks:', err);
          }

          const newNextScheduled = nextScheduled + intervalToUse;
          chrome.storage.local.set({ [STORAGE_KEY]: newNextScheduled }, () => {
             // We don't schedule the next one here anymore. The page reload will restart the script.
          });
          
          try {
            // Avoid reloading within 5 minutes of the daily notification time to prevent spawning new instances near the daily run
            try {
              if (this._dailyNotificationTime) {
                const [dh, dm] = String(this._dailyNotificationTime).split(':').map(Number);
                const nowDt = new Date();
                const candidate = new Date(nowDt.getFullYear(), nowDt.getMonth(), nowDt.getDate(), dh, dm, 0, 0);
                const diffMs = Math.abs(candidate.getTime() - Date.now());
                const FIVE_MIN_MS = 5 * 60 * 1000;
                if (diffMs < FIVE_MIN_MS) {
                  this.log('Skipping reload because it is within 5 minutes of daily notification time.');
                  // postpone the reload - schedule next run later
                  const postponed = Date.now() + intervalToUse;
                  chrome.storage.local.set({ [STORAGE_KEY]: postponed });
                  this._lastScheduledSkip = true;
                } else {
                  window.location.reload();
                }
              } else {
                window.location.reload();
              }
            } catch(e) {
              this.error('Reload check failed, attempting reload anyway', e);
              window.location.reload();
            }
          } catch (e) { this.error('Reload failed:', e); }

        }, delay);
      };

      scheduleNext();

      if (config.dailyReport !== 'no') this.scheduleDailyNotification();
      this.log(`Monitor started, refresh every ${intervalToUse/60000} minutes (configured ${refreshInterval/60000} minutes)`);
    });
  }

  stop() {
    if (this.checkInterval) { clearInterval(this.checkInterval); this.checkInterval = null; }
    if (this._dailyTimerId) { clearTimeout(this._dailyTimerId); this._dailyTimerId = null; }
    this.isChecking = false;
    this.log('Monitor stopped');
  }

  async restart() {
    this.log('Restarting monitor on request...');
    try {
        // Clear in-memory timers (main and daily) — note main timer is stored in checkInterval
        if (this.checkInterval) { clearTimeout(this.checkInterval); this.checkInterval = null; }
        if (this._timerId) { clearTimeout(this._timerId); this._timerId = null; }
        if (this._dailyTimerId) { clearTimeout(this._dailyTimerId); this._dailyTimerId = null; }
    } catch (e) {
        console.warn('Restart cleanup error:', e);
    }

    // Reset any stored timestamps so we don't reuse the old next-run baseline
    this._lastCheck = null;
    this._lastSummaryTs = null;

    console.log('Monitor fully reset — starting new cycle from now.');
    try {
      // Remove persisted next-run so start() will compute a fresh schedule from now
      const STORAGE_KEY = 'monitorNextScheduledTime';
      chrome.storage.local.remove([STORAGE_KEY], () => {
        // Use start() (the class's actual bootstrap) to re-initialize
        this.start().catch(err => this.error('restart: start() failed', err));
      });
    } catch (err) {
      this.error('restart: failed to clear persisted schedule or start', err);
    }
  }

  // interim summary (manual request)
  async handleInterimSummaryRequest() {
    this.log('Interim summary requested');
    await autoScrollToFullBottom();
    const summary = await this.computeRewardsSinceBaseline();
    const currentValues = this.getCurrentValues();
    if (!summary) { this.error('Interim summary aborted: could not compute summary'); throw new Error('No summary computed'); }

    const lines = [];
    lines.push(`📅 Interim Summary (${summary.from} → ${summary.to})`);

    // --- Rewards Earned So Far ---
    try {
      lines.push('');
      lines.push(`🎁 Rewards Earned So Far: +${summary.rewardPointsTotal} pts`);
    } catch (err) {
      this.warn('Rewards Earned So Far section failed:', err);
    }

    // --- Average Daily Rewards (Past 7 summaries) ---
    try {
      const historyKey = 'dailyRewardHistory';
      let history = await new Promise(res => chrome.storage.local.get([historyKey], r => res(r?.[historyKey] || [])));

      // Compute average without adding current period
      const sum = history.reduce((a, b) => a + b, 0);
      const avg = history.length > 0 ? (sum / history.length) : 0;
      const avgPts = Math.round(avg);
      const avgCount = history.length;

      lines.push(`🎁 Average Daily Rewards (Past ${avgCount} summaries): +${avgPts} pts/day — based on last ${avgCount} summaries`);
    } catch (err) {
      this.warn('Average Daily Rewards section failed:', err);
    }

    // --- Models Close to 🎁 ---
    try {
      lines.push('');
      let closeToGiftCount = 0;
      
      const allModels = Object.values(currentValues.models || {});
      for (const m of allModels) {
        const downloads = Number(m.downloads || 0);
        const prints = Number(m.prints || 0);
        const total = this.calculateDownloadsEquivalent(downloads, prints);
        const next = this.nextRewardDownloads(total);
        const remaining = Math.max(0, next - total);
        if (remaining <= 2) closeToGiftCount++;
      }
      
      lines.push(`⚙️ Models Close to 🎁: ${closeToGiftCount > 0 ? closeToGiftCount : 'none'}`);
    } catch (err) {
      this.warn('Models Close to 🎁 section failed:', err);
    }

    // --- Boosts Received So Far ---
    try {
      lines.push('');
      lines.push(`⚡ Boosts Received So Far: +${summary.dailyBoosts}`);
    } catch (err) {
      this.warn('Boosts Received So Far section failed:', err);
    }

    // --- Total Downloads So Far ---
    try {
      lines.push('');
      const weightedTotal = summary.dailyDownloads + 2 * summary.dailyPrints;
      lines.push(`⬇️ Total Downloads So Far (downloads + 2X prints): +${weightedTotal}`);
    } catch (err) {
      this.warn('Total Downloads So Far section failed:', err);
    }

    // --- Models per Reward Tier ---
    try {
      lines.push('');
      const tierCounts = { 1: 0, 2: 0, 3: 0, 4: 0 };

      const allModels = Object.values(currentValues.models || {});
      for (const m of allModels) {
        const downloads = Number(m.downloads || 0);
        const prints = Number(m.prints || 0);
        const tier = this.getRewardCategory(downloads, prints);
        tierCounts[tier]++;
      }

      const totalModels = allModels.length || 0;
      lines.push('📊 Models per Reward Tier:');
      if (totalModels > 0) {
        for (let t = 1; t <= 4; t++) {
          const pct = totalModels > 0 ? ((tierCounts[t] / totalModels) * 100).toFixed(0) : '0';
          let label;
          if (t === 1) label = '(0–49)';
          else if (t === 2) label = '(50–499)';
          else if (t === 3) label = '(500–999)';
          else label = '(1000+)';
          lines.push(`  Tier ${t} ${label}: ${tierCounts[t]} (${pct}%)`);
        }
      } else {
        lines.push(' (no models found)');
      }
    } catch (err) {
      this.warn('Models per Reward Tier section failed:', err);
    }
    
    // --- Top 10 Models ---
    try {
      lines.push('');
      const allChanges = Object.values(summary.modelChanges);
      
      const weightedChanges = allChanges.map(m => {
        const weightedDownloadsToday = (m.downloadsGained || 0) + 2 * (m.printsGained || 0);
        const totalWeighted = (m.currentDownloads || 0) + 2 * (m.currentPrints || 0);
        
        return {
          name: m.name,
          weightedDownloadsToday,
          totalWeighted
        };
      })
      .filter(m => m.weightedDownloadsToday > 0);
      
      weightedChanges.sort((a, b) => {
        if (a.weightedDownloadsToday !== b.weightedDownloadsToday) {
          return b.weightedDownloadsToday - a.weightedDownloadsToday;
        }
        return a.name.localeCompare(b.name);
      });
      
      lines.push('🔝 Top 10 Models (Downloads + 2X Prints):');
      if (weightedChanges.length === 0) {
        lines.push('  No models with download or print activity today.');
      } else {
        weightedChanges.slice(0, 10).forEach((m, i) => {
          lines.push(`  ${i + 1}. ${m.name} — +${m.weightedDownloadsToday} (total ${m.totalWeighted})`);
        });
      }
    } catch (err) {
      this.warn('Top 10 Models section failed:', err);
    }

    const message = lines.join('\n');
    this.log('Interim message:', message);
    const sent = await this.sendTelegramMessage(message);
    if (!sent) { this.error('Interim summary: failed to send via Telegram'); throw new Error('Telegram send failed'); }
    this.log('Interim summary: sent successfully');
    return true;
  }

  // ---------------------------------------------------------------------------
  // NEW: Restore _compileAndSendDailySummary for 24-hour daily report
  // (REFORMATTED per user request)
  // ---------------------------------------------------------------------------
  async _compileAndSendDailySummary() {
    try {
      this.log('_compileAndSendDailySummary: starting daily report');
      const summary = await this.getDailySummary();
      if (!summary) {
        this.warn('_compileAndSendDailySummary: getDailySummary() returned null');
        return;
      }

      const lines = [];
      lines.push(`📅 Daily Summary (${summary.from} → ${summary.to})`);

      // --- Rewards Earned Today ---
      try {
        const totalRewards = summary.rewardPointsTotal || 0;
        lines.push('');
        lines.push(`🎁 Rewards Earned Today: +${totalRewards} pts`);
      } catch (err) {
        this.warn('Rewards Earned Today section failed:', err);
      }

      // --- Average Daily Rewards (Past 7 Days) ---
      try {
        const historyKey = 'dailyRewardHistory';
        let history = await new Promise(res => chrome.storage.local.get([historyKey], r => res(r?.[historyKey] || [])));
        
        // Add today's summary to history
        const totalRewards = summary.rewardPointsTotal || 0;
        history.push(totalRewards);
        if (history.length > 7) {
          history = history.slice(-7);
        }
        // Save updated history
        await new Promise(res => chrome.storage.local.set({ [historyKey]: history }, res));

        // Compute average for message
        const sum = history.reduce((a, b) => a + b, 0);
        const avg = history.length > 0 ? (sum / history.length) : 0;
        const avgPts = Math.round(avg);
        const avgCount = history.length;

        lines.push(`🎁 Average Daily Rewards (Past ${avgCount} summaries): +${avgPts} pts/day — based on last ${avgCount} summaries`);
      } catch (err) {
        this.warn('Average Daily Rewards section failed:', err);
      }

      // --- Models Close to 🎁 ---
      try {
        lines.push('');
        const currentValues = this.getCurrentValues() || {};
        const allModels = Object.values(currentValues.models || {});
        let closeToGiftCount = 0;
        
        for (const m of allModels) {
          const downloads = Number(m.downloads || 0);
          const prints = Number(m.prints || 0);
          const total = this.calculateDownloadsEquivalent(downloads, prints);
          const next = this.nextRewardDownloads(total);
          const remaining = Math.max(0, next - total);
          if (remaining <= 2) closeToGiftCount++;
        }
        
        lines.push(`⚙️ Models Close to 🎁: ${closeToGiftCount > 0 ? closeToGiftCount : 'none'}`);
      } catch (err) {
        this.warn('Models Close to 🎁 section failed:', err);
      }

      // --- Boosts Received Today ---
      try {
        lines.push('');
        const dailyBoosts = summary.dailyBoosts || 0;
        lines.push(`⚡ Boosts Received Today: +${dailyBoosts}`);
      } catch (err) {
        this.warn('Boosts Received Today section failed:', err);
      }

      // --- Total Downloads Today ---
      try {
        lines.push('');
        const weightedTotal = (summary.dailyDownloads || 0) + 2 * (summary.dailyPrints || 0);
        lines.push(`⬇️ Total Downloads Today (downloads + 2X prints): +${weightedTotal}`);
      } catch (err) {
        this.warn('Total Downloads Today section failed:', err);
      }

      // --- Models per Reward Tier ---
      try {
        lines.push('');
        const currentValues = this.getCurrentValues() || {};
        const allModels = Object.values(currentValues.models || {});
        const tierCounts = { 1: 0, 2: 0, 3: 0, 4: 0 };

        for (const m of allModels) {
          const downloads = Number(m.downloads || 0);
          const prints = Number(m.prints || 0);
          const tier = this.getRewardCategory(downloads, prints);
          tierCounts[tier]++;
        }

        const totalModels = allModels.length || 0;
        lines.push('📊 Models per Reward Tier:');
        if (totalModels > 0) {
          for (let t = 1; t <= 4; t++) {
            const pct = totalModels > 0 ? ((tierCounts[t] / totalModels) * 100).toFixed(0) : '0';
            let label;
            if (t === 1) label = '(0–49)';
            else if (t === 2) label = '(50–499)';
            else if (t === 3) label = '(500–999)';
            else label = '(1000+)';
            lines.push(`  Tier ${t} ${label}: ${tierCounts[t]} (${pct}%)`);
          }
        } else {
          lines.push(' (no models found)');
        }
      } catch (err) {
        this.warn('Models per Reward Tier section failed:', err);
      }
      
      // --- Top 10 Models ---
      try {
        lines.push('');
        // Get the full list of changes from the summary object
        const allChanges = Object.values(summary.modelChanges || {});
        
        const weightedChanges = allChanges.map(m => {
          // m.downloadsGained/printsGained/boostsGained are from modelChanges
          const weightedDownloadsToday = (m.downloadsGained || 0) + 2 * (m.printsGained || 0);
          // m.currentDownloads/currentPrints are also from modelChanges
          const totalWeighted = (m.currentDownloads || 0) + 2 * (m.currentPrints || 0);
          
          return {
            name: m.name,
            weightedDownloadsToday,
            totalWeighted
          };
        })
        .filter(m => m.weightedDownloadsToday > 0); // Only show models with activity
        
        // Sort: desc by weightedDownloads, then asc by name
        weightedChanges.sort((a, b) => {
          if (a.weightedDownloadsToday !== b.weightedDownloadsToday) {
            return b.weightedDownloadsToday - a.weightedDownloadsToday;
          }
          return a.name.localeCompare(b.name);
        });
        
        lines.push('🔝 Top 10 Models (Downloads + 2X Prints):');
        if (weightedChanges.length === 0) {
          lines.push('  No models with download or print activity today.');
        } else {
          weightedChanges.slice(0, 10).forEach((m, i) => {
            // 1. Christmas Box — +5 (total 17)
            lines.push(`  ${i + 1}. ${m.name} — +${m.weightedDownloadsToday} (total ${m.totalWeighted})`);
          });
        }
      } catch (err) {
        this.warn('Top 10 Models section failed:', err);
      }

      // --- Send Message ---
      const message = lines.join('\n');
      this.log('_compileAndSendDailySummary: message length =', message.length);
      await this.sendTelegramMessage(message);
      this.log('_compileAndSendDailySummary: daily summary sent successfully (with new format)');

      const periodKey = await this.getCurrentPeriodKey();
      const snapshot = { models: this.getCurrentValues().models || {}, points: summary.points || 0, timestamp: Date.now() };
      chrome.storage.local.set({ [this._lastSuccessfulKey]: { state:'SENT', owner:this._instanceId, sentAt:Date.now(), periodKey, snapshot, rewardPointsTotal: summary.rewardPointsTotal } });
    } catch (err) {
      this.error('_compileAndSendDailySummary error:', err);
    }
  }
}

// Startup
this.log = console.log.bind(console);
this.warn = console.warn.bind(console);
this.error = console.error.bind(console);

console.log('Initializing monitor...');
const monitor = new ValueMonitor();
monitor.start();

// Listen for popup messages
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.type === 'INTERIM_SUMMARY_REQUEST') {
    monitor.handleInterimSummaryRequest().then(()=>sendResponse({ok:true})).catch(err=>{ console.error('interim summary error', err); sendResponse({ok:false, error: err?.message}); });
    return true;
  }
  if (msg?.type === 'REFRESH_INTERVAL_UPDATED') {
    monitor.restart().then(()=>sendResponse({ok:true})).catch(err=>{ console.error('restart error', err); sendResponse({ok:false, error: err?.message}); });
    return true;
  }
  if (msg?.type === 'CONFIG_SAVED') {
    chrome.storage.sync.get(['notifySummaryMode'], cfg => { monitor.notifySummaryMode = !!(cfg?.notifySummaryMode); monitor.log('CONFIG_SAVAGED received. notifySummaryMode =', monitor.notifySummaryMode); monitor.restart().then(()=>sendResponse({ok:true})).catch(err=>sendResponse({ok:false, error: err?.message})); });
    return true;
  }
});