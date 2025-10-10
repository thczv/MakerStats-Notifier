

const ITERATION = 'Iteration 20.4.1';
console.log(`Initializing monitor — ${ITERATION}`);

const autoScrollToFullBottom = ({ step = 600, delay = 250, settle = 800 } = {}) => new Promise(resolve => {
  let timer;
  const scrollAndCheck = () => {
    window.scrollBy(0, step);
    const currentHeight = document.body.scrollHeight;
    const atBottom = (window.innerHeight + window.scrollY) >= (currentHeight - 2);
    if (atBottom) {
      clearInterval(timer);
      setTimeout(() => {
        if (document.body.scrollHeight > currentHeight) timer = setInterval(scrollAndCheck, delay);
        else resolve();
      }, settle);
    }
  };
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
    this._dailyRunningRewardKey = 'dailyRunningRewardPoints';
    this._dailyRunningRewardDayKey = 'dailyRunningRewardDay';
    this._dailyLockTimeoutMs = 2 * 60 * 1000;
    this._dailyMaxPreSendRetries = 5;
    this._dailyPreSendBaseBackoffMs = 300;
    this._dailyScheduleJitterMs = 30 * 1000;
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

  // storage lock helpers
  async acquireDailyLock(timeoutMs = this._dailyLockTimeoutMs) {
    const now = Date.now();
    return new Promise(resolve => chrome.storage.local.get([this._dailyLockKey], res => {
      const lock = res?.[this._dailyLockKey] || null;
      if (!lock || (now - lock.ts) > timeoutMs) {
        if (lock && (now - lock.ts) > timeoutMs) {
          chrome.storage.local.remove([this._dailyLockKey], () => {
            const newLock = { ts: now, owner: this._instanceId };
            chrome.storage.local.set({ [this._dailyLockKey]: newLock }, () => {
              chrome.storage.local.get([this._dailyLockKey], r2 => {
                const confirmed = r2?.[this._dailyLockKey]?.owner === this._instanceId;
                this.log('acquireDailyLock (force unlock) result', { confirmed, owner: r2?.[this._dailyLockKey]?.owner, instance: this._instanceId });
                resolve(confirmed);
              });
            });
          });
        } else {
          const newLock = { ts: now, owner: this._instanceId };
          chrome.storage.local.set({ [this._dailyLockKey]: newLock }, () => {
            chrome.storage.local.get([this._dailyLockKey], r2 => {
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
    return new Promise(resolve => chrome.storage.local.get([this._dailyLockKey], res => {
      const lock = res?.[this._dailyLockKey] || null;
      if (lock && lock.owner === this._instanceId) {
        chrome.storage.local.remove([this._dailyLockKey], () => { this.log('releaseDailyLock: released by', this._instanceId); resolve(true); });
      } else resolve(false);
    }));
  }

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

  // robust daily summary computation and storage
  async getDailySummary() {
    const currentValues = this.getCurrentValues();
    if (!currentValues) { this.error('Unable to get current values'); return null; }
    this.log('getDailySummary START — now:', new Date().toISOString());
    this.log('getDailySummary: currentValues.models count =', Object.keys(currentValues.models || {}).length, 'timestamp=', new Date(currentValues.timestamp).toISOString());

    const [previousDayRaw, maxStaleMs] = await Promise.all([
      new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r => res(r?.[this._dailyStatsKey] || null))),
      new Promise(res => chrome.storage.sync.get(['dailyFallbackMaxAgeMs'], cfg => {
        const cfgVal = cfg?.dailyFallbackMaxAgeMs; res(Number.isFinite(cfgVal) ? cfgVal : (this._defaultFallbackHours*60*60*1000));
      }))
    ]);

    let raw = previousDayRaw;
    if (!raw) {
      for (let i=0;i<3 && !raw;i++){ this.log('getDailySummary: dailyStats missing — retrying read (attempt)', i+1); await new Promise(r=>setTimeout(r,1000)); raw = await new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r2 => res(r2?.[this._dailyStatsKey] || null))); }
    }
    if (raw) this.log('getDailySummary: read dailyStats from storage:', { ts: new Date(raw.timestamp).toISOString(), ageMs: Date.now()-raw.timestamp, modelsCount: Object.keys(raw.models || {}).length, points: raw.points, owner: raw.owner || null, periodKey: raw.periodKey || null });
    else this.log('getDailySummary: no dailyStats found in storage after retries.');

    const ONE_DAY_MS = 24*60*60*1000;
    let previousDay = null;
    if (raw) {
      const ageMs = Date.now() - raw.timestamp;
      if (ageMs <= ONE_DAY_MS) previousDay = raw, this.log('getDailySummary: using fresh snapshot', new Date(raw.timestamp).toISOString());
      else if (ageMs <= maxStaleMs) previousDay = raw, this.warn('getDailySummary: using STALE snapshot as fallback', new Date(raw.timestamp).toISOString(), `ageMs=${ageMs}`);
      else this.log('getDailySummary: snapshot too old, treating as missing', new Date(raw.timestamp).toISOString(), `ageMs=${ageMs}`);
    }

    if (!previousDay) {
      this.log('getDailySummary: No previous day data available or snapshot unusable. Writing current snapshot and returning empty summary.');
      const periodKey = await this.getCurrentPeriodKey();
      chrome.storage.local.set({ [this._dailyStatsKey]: { models: currentValues.models, points: currentValues.points, timestamp: Date.now(), owner: this._instanceId, periodKey } }, () => {
        this.log('getDailySummary: stored dailyStats ts=', new Date().toISOString(), 'modelsCount=', Object.keys(currentValues.models || {}).length, 'owner=', this._instanceId, 'periodKey=', periodKey);
      });
      chrome.storage.local.set({ [this._dailyRunningRewardKey]: 0, [this._dailyRunningRewardDayKey]: periodKey, [this._lastSuccessfulKey]: { state:'SENT', owner:this._instanceId, sentAt:Date.now(), periodKey, snapshot:{ models: currentValues.models, points: currentValues.points, timestamp: Date.now() }, rewardPointsTotal:0 } });
      return { dailyDownloads:0, dailyPrints:0, points: currentValues.points, pointsGained:0, top5Downloads:[], top5Prints:[], rewardsEarned:[], rewardPointsTotal:0, from:new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }), to:new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }) };
    }

    // compare
    const modelChanges = {};
    this.log('getDailySummary: comparing models; previousDay.models count', Object.keys(previousDay.models || {}).length);
    for (const [id, current] of Object.entries(currentValues.models)) {
      let previous = previousDay?.models?.[id] || null;
      if (!previous && current.permalink) previous = Object.values(previousDay.models || {}).find(m => m?.permalink === current.permalink) || null;
      if (!previous && current.name) { const norm = current.name.trim().toLowerCase(); previous = Object.values(previousDay.models || {}).find(m => m?.name?.trim().toLowerCase() === norm) || null; if (previous) this.log('getDailySummary: matched previous by name', { id, name: current.name }); }
      if (!previous) { this.log('New model found:', current.name, 'id=', id, 'permalink=', current.permalink); continue; }

      const prevDownloads = Number(previous.downloads || 0), prevPrints = Number(previous.prints || 0), currDownloads = Number(current.downloads || 0), currPrints = Number(current.prints || 0);
      let downloadsGained = currDownloads - prevDownloads, printsGained = currPrints - prevPrints;
      if (downloadsGained <= 0 && printsGained <= 0) continue;
      if (downloadsGained > this._suspiciousDeltaLimit || printsGained > this._suspiciousDeltaLimit) { this.warn('Suspiciously large delta detected, skipping reward calculation for', { id, name: current.name, downloadsGained, printsGained, prevTs: previous.timestamp || null }); continue; }

      modelChanges[id] = { id, name: current.name, downloadsGained, printsGained, previousDownloads: prevDownloads, previousPrints: prevPrints, currentDownloads: currDownloads, currentPrints: currPrints, permalink: current.permalink || previous?.permalink || null };
    }

    const dailyDownloads = Object.values(modelChanges).reduce((s,m)=>s+m.downloadsGained,0);
    const dailyPrints = Object.values(modelChanges).reduce((s,m)=>s+m.printsGained,0);
    const top5Downloads = Object.values(modelChanges).filter(m=>m.downloadsGained>0).sort((a,b)=>b.downloadsGained-a.downloadsGained).slice(0,5);
    const top5Prints = Object.values(modelChanges).filter(m=>m.printsGained>0).sort((a,b)=>b.printsGained-a.printsGained).slice(0,5);

    // compute rewards
    const rewardsEarned = []; let rewardPointsTotal = 0;
    for (const m of Object.values(modelChanges)) {
      const prevDownloadsTotal = this.calculateDownloadsEquivalent(m.previousDownloads, m.previousPrints);
      const currDownloadsTotal = this.calculateDownloadsEquivalent(m.currentDownloads, m.currentPrints);
      let cursor = prevDownloadsTotal; const thresholdsHit = []; const maxThresholdsPerModel = 200; let thresholdsCount = 0;
      while (cursor < currDownloadsTotal && thresholdsCount < maxThresholdsPerModel) {
        const interval = this.getRewardInterval(cursor);
        const mod = cursor % interval;
        const nextThreshold = (cursor === 0 || mod === 0) ? cursor + interval : cursor + (interval - mod);
        if (nextThreshold <= currDownloadsTotal) {
          const rewardPoints = this.getRewardPointsForDownloads(nextThreshold);
          thresholdsHit.push({ threshold: nextThreshold, rewardPoints });
          rewardPointsTotal += rewardPoints;
          cursor = nextThreshold; thresholdsCount++;
        } else break;
      }
      if (thresholdsHit.length) rewardsEarned.push({ id:m.id, name:m.name, thresholds:thresholdsHit.map(t=>t.threshold), rewardPointsTotalForModel:thresholdsHit.reduce((s,t)=>s+t.rewardPoints,0) });
    }

    const periodKey = await this.getCurrentPeriodKey();
    chrome.storage.local.set({ [this._dailyStatsKey]: { models: currentValues.models, points: currentValues.points, timestamp: Date.now(), owner: this._instanceId, periodKey } }, () => {
      this.log('getDailySummary: updated dailyStats ts=', new Date().toISOString(), 'modelsCount=', Object.keys(currentValues.models || {}).length, 'owner=', this._instanceId, 'periodKey', periodKey);
    });
    chrome.storage.local.set({ [this._dailyRunningRewardKey]: 0, [this._dailyRunningRewardDayKey]: periodKey, [this._lastSuccessfulKey]: { state:'SENT', owner:this._instanceId, sentAt:Date.now(), periodKey, snapshot:{ models: currentValues.models, points: currentValues.points, timestamp: Date.now() }, rewardPointsTotal:0 } });

    return { dailyDownloads, dailyPrints, points: currentValues.points, pointsGained: currentValues.points - previousDay.points, top5Downloads, top5Prints, rewardsEarned, rewardPointsTotal, from: new Date(previousDay.timestamp).toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }), to: new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }) };
  }

  // schedule daily report with locking/claiming
  scheduleDailyNotification() {
    if (this._dailyTimerId) { clearTimeout(this._dailyTimerId); this._dailyTimerId = null; }
    chrome.storage.sync.get(['dailyReport','dailyNotificationTime'], (config) => {
      const dailyReport = config.dailyReport || 'yes';
      if (dailyReport === 'no') { this.log('Daily report disabled'); return; }
      const dailyTime = config.dailyNotificationTime || '12:00';
      const [hour, minute] = dailyTime.split(':').map(Number);
      const now = new Date(); const nextNotification = new Date(); nextNotification.setHours(hour, minute, 0, 0); if (nextNotification <= now) nextNotification.setDate(nextNotification.getDate()+1);
      const jitter = Math.floor((Math.random()*2 -1) * this._dailyScheduleJitterMs);
      const delay = Math.max(0, nextNotification - now + jitter);
      this.log(`Daily report scheduled for: ${new Date(Date.now()+delay)}. Delay: ${delay}ms; jitterMs=${jitter}`);
      this._dailyTimerId = setTimeout(async () => {
        const startTime = Date.now(); this.log(`scheduleDailyNotification: firing attempt at ${new Date().toISOString()}`);
        const claimantKey = this._lastSuccessfulKey; let clearPending=false, wrotePending=false, periodKey=null;
        const gotLock = await this.acquireDailyLock();
        if (!gotLock) { this.log('scheduleDailyNotification: could not acquire lock, another instance likely running. Aborting this attempt.'); this.scheduleDailyNotification(); return; }
        try {
          const latestAfterLock = await new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r => res(r?.[this._dailyStatsKey] || null)));
          if (latestAfterLock && latestAfterLock.timestamp >= startTime) { this.log('scheduleDailyNotification: dailyStats updated by another instance after lock; aborting this send.'); return; }
          const nowMs = Date.now(); periodKey = await this.getCurrentPeriodKey();
          const claimResult = await new Promise(res => {
            chrome.storage.local.get([claimantKey], r => {
              const existing = r?.[claimantKey] || null;
              if (existing && existing.state === 'SENT' && existing.periodKey === periodKey) { res({ ok:false, reason:'already-sent', existing }); return; }
              const PENDING_STALE_MS = 5 * 60 * 1000;
              if (existing && existing.state === 'PENDING' && (nowMs - (existing.claimAt || 0)) < PENDING_STALE_MS && existing.periodKey === periodKey) { res({ ok:false, reason:'pending-claim', existing }); return; }
              const claim = { state:'PENDING', owner:this._instanceId, claimAt: nowMs, periodKey };
              chrome.storage.local.set({ [claimantKey]: claim }, () => { chrome.storage.local.get([claimantKey], v => { const confirmed = v?.[claimantKey]?.owner === this._instanceId && v[claimantKey].state === 'PENDING' && v[claimantKey].periodKey === periodKey; res({ ok:confirmed, claim }); }); });
            });
          });
          if (!claimResult.ok) { this.log('scheduleDailyNotification: aborting send due to claim failure', claimResult.reason, claimResult.existing); return; }
          wrotePending = true;
          const okToSend = await this.preSendCheckAndMaybeWait(startTime);
          if (!okToSend) { this.log('scheduleDailyNotification: preSendCheck indicates newer snapshot; aborting this send.'); clearPending = true; return; }
          this.log(`Sending daily report: ${new Date()}`);
          const summary = await this.getDailySummary();
          if (summary) {
            let rewardsSection = 'No rewards earned in the prior 24 hours';
            if (summary.rewardsEarned?.length) rewardsSection = summary.rewardsEarned.map(r => `${r.name}: +${r.rewardPointsTotalForModel} points (thresholds: ${r.thresholds.join(', ')})`).join('\n');
            const totalEquivalent = summary.dailyDownloads + (summary.dailyPrints * 2);
            const topDownloadsList = summary.top5Downloads.length ? summary.top5Downloads.map((m,i)=>`${i+1}. ${m.name}: +${m.downloadsGained}`).join('\n') : 'No new downloads today';
            const topPrintsList = summary.top5Prints.length ? summary.top5Prints.map((m,i)=>`${i+1}. ${m.name}: +${m.printsGained}`).join('\n') : 'No new prints today';
            const message = `
📊 24-Hour Summary (${summary.from} - ${summary.to}):

Total Downloads: ${totalEquivalent} [total of downloads + 2x prints]

🏆 Today's Most Downloaded:
${topDownloadsList}

🖨️ Today's Most Printed:
${topPrintsList}

🎁 Rewards earned in last 24 hours:
${rewardsSection}
Total Reward Points: ${summary.rewardPointsTotal}
`.trim();
            const sent = await this.sendTelegramMessage(message);
            if (sent) {
              const sentAt = Date.now();
              const runningRewardPoints = await new Promise(res => chrome.storage.local.get([this._dailyRunningRewardKey], r => res(r?.[this._dailyRunningRewardKey] || 0)));
              const finalRecord = { state:'SENT', owner:this._instanceId, sentAt, periodKey, snapshot:{ models: summary.top5Downloads.concat(summary.top5Prints).reduce((acc,m)=>{acc[m.id]=m;return acc;},{}), points: summary.points, timestamp: Date.now() }, rewardPointsTotal: runningRewardPoints };
              await new Promise(resolve => chrome.storage.local.set({ [this._lastSuccessfulKey]: finalRecord }, () => { this.log('scheduleDailyNotification: wrote lastSuccessfulDailyReport SENT by', this._instanceId, 'sentAt=', new Date(sentAt).toISOString(), 'periodKey=', periodKey); resolve(); }));
              try { chrome.storage.local.set({ [this._dailyRunningRewardKey]:0, [this._dailyRunningRewardDayKey]: periodKey }, () => { this.log('scheduleDailyNotification: reset running reward counter for period', { periodKey }); }); } catch (err) { this.error('Error resetting running reward counter for period', err); }
            } else clearPending = true;
          } else clearPending = true;
        } finally {
          try {
            if (clearPending && claimantKey) {
              chrome.storage.local.get([claimantKey], r => {
                const existing = r?.[claimantKey] || null;
                if (existing && existing.state === 'PENDING' && existing.owner === this._instanceId) chrome.storage.local.remove([claimantKey], () => { this.log('scheduleDailyNotification: cleared PENDING claim during cleanup'); });
                else this.log('scheduleDailyNotification: skip clearing PENDING (not owner or not pending)', { existing });
              });
            }
          } finally {
            await this.releaseDailyLock();
            this.scheduleDailyNotification();
          }
        }
      }, delay);
    });
  }

  // main periodic check (per-model messages or summary)
  async checkAndNotify() {
    if (this.isChecking) { this.log('Check already in progress, skipping...'); return; }
    this.log(`checkAndNotify start — ${ITERATION}`); this.isChecking = true;
    try {
      this.log('Starting change check...');
      let anyNotification = false;
      const currentValues = this.getCurrentValues();
      if (!currentValues) { this.log('No current values found'); await this.savePreviousValues({}); return; }
      if (!this.previousValues) await this.loadPreviousValues();
      if (!this.previousValues) { this.log('First run or no previous values, saving initial values'); this.previousValues = currentValues; await this.savePreviousValues(currentValues); return; }
      if (this.previousValues && !this.previousValues.models) { this.previousValues.models = {}; await this.savePreviousValues(this.previousValues); }
      if (currentValues.points > (this.previousValues.points || 0)) this.log('Global account points increased, ignoring for per-model-only Telegram notifications.');

      const modelsActivity = []; let modelUpdateCount = 0;
      for (const [id, current] of Object.entries(currentValues.models)) {
        const previous = this.previousValues.models[id];
        if (!previous) continue;
        const previousDownloadsRaw = Number(previous.downloads) || 0, previousPrints = Number(previous.prints) || 0, previousBoosts = Number(previous.boosts) || 0;
        const previousDownloadsTotal = this.calculateDownloadsEquivalent(previousDownloadsRaw, previousPrints);
        const currentDownloadsRaw = Number(current.downloads) || 0, currentPrints = Number(current.prints) || 0, currentBoosts = Number(current.boosts) || 0;
        const currentDownloadsTotal = this.calculateDownloadsEquivalent(currentDownloadsRaw, currentPrints);
        const downloadsDeltaRaw = currentDownloadsRaw - previousDownloadsRaw, printsDelta = currentPrints - previousPrints, boostsDelta = currentBoosts - previousBoosts;
        const downloadsDeltaEquivalent = downloadsDeltaRaw + (printsDelta * 2);
        const hasActivity = (downloadsDeltaRaw !== 0) || (printsDelta !== 0) || (boostsDelta > 0);
        if (hasActivity) modelUpdateCount++;
        const modelSummary = { id, name: current.name, imageUrl: current.imageUrl, downloadsDeltaRaw, printsDelta, boostsDelta, previousDownloadsTotal, currentDownloadsTotal, downloadsDeltaEquivalent, rewards: [] };

        if (currentDownloadsTotal > previousDownloadsTotal) {
          let cursor = previousDownloadsTotal, maxRewardsToReport = 50, rewardsFound = 0;
          while (cursor < currentDownloadsTotal && rewardsFound < maxRewardsToReport) {
            const interval = this.getRewardInterval(cursor), mod = cursor % interval;
            const nextThreshold = (cursor === 0 || mod === 0) ? cursor + interval : cursor + (interval - mod);
            this.log(`REWARD-LOOP ${current.name} cursor=${cursor} interval=${interval} nextThreshold=${nextThreshold} currDownloads=${currentDownloadsTotal}`);
            if (nextThreshold <= currentDownloadsTotal) { const rewardPoints = this.getRewardPointsForDownloads(nextThreshold); modelSummary.rewards.push({ thresholdDownloads: nextThreshold, points: rewardPoints }); cursor = nextThreshold; rewardsFound++; } else break;
          }
          if (rewardsFound >= maxRewardsToReport) this.log(`Many rewards earned for ${current.name}. Listed first ${maxRewardsToReport} in summary.`);
        }

        const boostOnly = (boostsDelta > 0) && (downloadsDeltaRaw === 0 && printsDelta === 0 && modelSummary.rewards.length === 0);
        if (!this.notifySummaryMode) {
          if (boostOnly) {
            const lines = [];
            lines.push(`⚡ Boost Update for: ${current.name}`, '', `⚡ Boosts: +${boostsDelta} (now ${currentBoosts})`);
            const message = lines.join('\n');
            this.log('MESSAGE-BRANCH', { iteration: ITERATION, name: current.name, branch: 'boost-only', downloadsDeltaEquivalent, boostsDelta, rewardsFound: modelSummary.rewards.length });
            this.log(`Sending boost-only message for ${current.name}`);
            const sent = await this.sendTelegramMessageWithPhoto(message, modelSummary.imageUrl);
            anyNotification = true; continue;
          }

          const hasActivity2 = (downloadsDeltaRaw !== 0) || (printsDelta !== 0) || (modelSummary.rewards.length > 0) || (boostsDelta > 0);
          if (hasActivity2) {
            this.log('MESSAGE-BRANCH', { iteration: ITERATION, name: current.name, branch: 'milestone', downloadsDeltaEquivalent, boostsDelta, rewardsFound: modelSummary.rewards.length });
            const lines = []; const equivalentTotal = currentDownloadsTotal;
            lines.push(`📦 Update for: ${current.name}`, '', `${downloadsDeltaEquivalent > 0 ? '+' : ''}${downloadsDeltaEquivalent} Downloads (total ${equivalentTotal})`, '');
            if (modelSummary.rewards.length > 0) { modelSummary.rewards.forEach(r => lines.push(`🎁 Reward Earned! +${r.points} points at ${r.thresholdDownloads} downloads`)); lines.push(''); }
            const nextThresholdAfterCurrent = this.nextRewardDownloads(equivalentTotal);
            const downloadsUntilNext = Math.max(0, nextThresholdAfterCurrent - equivalentTotal);
            lines.push(`🎯 Next Reward: ${downloadsUntilNext} more downloads (${nextThresholdAfterCurrent} total)`, '', `🔁 Reward Interval: every ${this.getRewardInterval(equivalentTotal)} downloads`);
            if (boostsDelta > 0) lines.push('', `⚡ Boosts: +${boostsDelta} (now ${currentBoosts})`);
            let warning = '';
            if (Math.abs(downloadsDeltaRaw) > this._suspiciousDeltaLimit || Math.abs(printsDelta) > this._suspiciousDeltaLimit) {
              warning = "\n\n⚠️ The number of downloads or prints during this period is very high. This could be because your model is very popular (good job!). Or it could be an error. You may want to shorten the refresh interval.";
            }
            const message = lines.join('\n') + warning;
            this.log(`Sending milestone message for ${current.name}`);
            const sent = await this.sendTelegramMessageWithPhoto(message, modelSummary.imageUrl);
            if (sent && modelSummary.rewards.length > 0) { const pts = modelSummary.rewards.reduce((s,r)=>s+r.points,0); await this._accumulateDailyRewardPoints(pts); }
            anyNotification = true;
          }
        } else {
          const hasActivity3 = (downloadsDeltaRaw !== 0) || (printsDelta !== 0) || (modelSummary.rewards.length > 0) || (boostsDelta > 0);
          if (hasActivity3) modelsActivity.push({ id, name: current.name, downloadsDeltaEquivalent, currentDownloadsTotal: currentDownloadsTotal, rewardPointsForThisModel: modelSummary.rewards.reduce((s,r)=>s+r.points,0), boostsDelta });
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
          await this.sendTelegramMessage("No new prints or downloads found."); anyNotification = true;
          const prevString = JSON.stringify(this.previousValues||{}), currString = JSON.stringify(currentValues||{});
          if (prevString !== currString) { this.previousValues = currentValues; await this.savePreviousValues(currentValues); }
          this.isChecking = false; return;
        }

        const totalEquivalent = modelsActivity.reduce((s,m)=>s + (m.downloadsDeltaEquivalent||0),0);
        const rewardPointsThisRun = modelsActivity.reduce((s,m)=>s + (m.rewardPointsForThisModel||0),0);
        await this._accumulateDailyRewardPoints(rewardPointsThisRun);

        const persisted = await new Promise(res => chrome.storage.local.get([this._dailyRunningRewardKey, this._dailyRunningRewardDayKey, this._dailyStatsKey, this._lastSuccessfulKey], r => res(r)));
        let running = Number.isFinite(persisted?.[this._dailyRunningRewardKey]) ? persisted[this._dailyRunningRewardKey] : 0;
        const runningDay = persisted?.[this._dailyRunningRewardDayKey] || null;
        const lastDaily = persisted?.[this._lastSuccessfulKey] || null;
        const lastDailyPoints = Number.isFinite(lastDaily?.rewardPointsTotal) ? lastDaily.rewardPointsTotal : 0;
        const currentPeriod = await this.getCurrentPeriodKey();

        if (!running || runningDay !== currentPeriod) {
          this.log('Periodic summary: running counter missing or mismatched; computing fallback via getDailySummary');
          try {
            const computed = await this.getDailySummary();
            if (computed && Number.isFinite(computed.rewardPointsTotal)) {
              running = computed.rewardPointsTotal;
              try { chrome.storage.local.set({ [this._dailyRunningRewardKey]: running, [this._dailyRunningRewardDayKey]: currentPeriod }, () => { this.log('Periodic summary: persisted computed running counter', { running, period: currentPeriod }); }); } catch (err) { this.warn('Periodic summary: failed to persist computed running counter', err); }
            } else this.warn('Periodic summary: computed fallback missing rewardPointsTotal; leaving persisted running as-is', { running, runningDay });
          } catch (err) { this.warn('Periodic summary: fallback computation failed', err); }
        }

        let rewardsToday = running - lastDailyPoints; if (rewardsToday < 0) rewardsToday = running;
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

        const message = headerLines.join('\n') + '\n' + spacedModels + '\n' + footerLines.join('\n');
        this.log('Aggregated summary message length:', message.length);
        const sent = await this.sendTelegramMessage(message); if (sent) anyNotification = true;
      } else {
        // per-model logic already executed inside loop
      }

      const prevString = JSON.stringify(this.previousValues || {}), currString = JSON.stringify(currentValues || {});
      if (prevString !== currString) { this.previousValues = currentValues; await this.savePreviousValues(currentValues); } else this.log('No changes detected, skipping savePreviousValues to reduce storage writes.');
      if (!anyNotification && !useSummaryMode) { const heartbeatMsg = 'No new prints or downloads found.'; this.log(heartbeatMsg); await this.sendTelegramMessage(heartbeatMsg); }
    } catch (err) { this.error('Error during check:', err); }
    finally { this.isChecking = false; }
  }

  // accumulate running points with lock and retries
  async _accumulateDailyRewardPoints(pointsToAdd, attempt = 1) {
    if (!Number.isFinite(pointsToAdd) || pointsToAdd <= 0) return;
    const MAX_ATTEMPTS = 3, BASE_DELAY_MS = 300, MAX_BACKOFF_MS = 3000, lockTimeoutMs = 1500;
    while (attempt <= MAX_ATTEMPTS) {
      const lockAcquired = await this.acquireDailyLock(lockTimeoutMs);
      if (!lockAcquired) {
        if (attempt >= MAX_ATTEMPTS) { this.warn(`_accumulateDailyRewardPoints: lock not acquired after ${MAX_ATTEMPTS} attempts; skipping this accumulation.`); return; }
        const backoff = Math.min(MAX_BACKOFF_MS, BASE_DELAY_MS * Math.pow(2, attempt-1));
        const jitter = Math.floor(Math.random() * Math.min(500, Math.floor(backoff/2)));
        const delay = backoff + jitter; this.log(`_accumulateDailyRewardPoints: lock not acquired (attempt ${attempt}). Waiting ${delay}ms then retrying.`); await new Promise(r=>setTimeout(r, delay)); attempt++; continue;
      }

      try {
        const periodKey = await this.getCurrentPeriodKey();
        const local = await new Promise(res => chrome.storage.local.get([this._dailyRunningRewardKey, this._dailyRunningRewardDayKey], r => res(r)));
        let running = (local && local[this._dailyRunningRewardKey]) ? local[this._dailyRunningRewardKey] : 0;
        const runningDay = (local && local[this._dailyRunningRewardDayKey]) ? local[this._dailyRunningRewardDayKey] : null;
        this.log('_accumulateDailyRewardPoints read', { running, runningDay, periodKey, pointsToAdd, attempt });

        if (runningDay && runningDay !== periodKey) {
          const runningDayTs = new Date(runningDay).getTime(), periodKeyTs = new Date(periodKey).getTime();
          if (Number.isFinite(runningDayTs) && Number.isFinite(periodKeyTs)) {
            if (runningDayTs > periodKeyTs) { this.log('_accumulateDailyRewardPoints: stored runningDay is in the future; resetting running to 0 and continuing', { runningDay, periodKey }); running = 0; }
            else { this.warn('_accumulateDailyRewardPoints: stored runningDay older than current period; aborting to avoid joining wrong window', { runningDay, periodKey }); return; }
          } else { this.log('_accumulateDailyRewardPoints: could not parse runningDay/periodKey; resetting running to 0 and continuing', { runningDay, periodKey }); running = 0; }
        }
        if (!runningDay) { this.log('_accumulateDailyRewardPoints resetting running counter for new period', { oldDay: runningDay, newPeriod: periodKey }); running = 0; }
        running += pointsToAdd;
        await new Promise(resolve => {
          chrome.storage.local.set({ [this._dailyRunningRewardKey]: running, [this._dailyRunningRewardDayKey]: periodKey }, (err) => {
            if (chrome.runtime.lastError || err) this.warn('Storage set failed (quota or other error):', chrome.runtime.lastError || err);
            else this.log(`_accumulateDailyRewardPoints: added ${pointsToAdd}, running=${running}, period=${periodKey}`);
            resolve();
          });
        });
        return;
      } finally { await this.releaseDailyLock(); }
    }
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
      const refreshInterval = config.refreshInterval || 900000;
      this.log(`Configured refresh interval: ${refreshInterval}ms`); this.log(`Notify summary mode: ${this.notifySummaryMode}`);
      let intervalToUse = refreshInterval; const ONE_HOUR = 60*60*1000; const COMPENSATION_MS = 60*1000;
      if (refreshInterval > ONE_HOUR) { intervalToUse = Math.max(0, refreshInterval - COMPENSATION_MS); this.log(`Interval adjusted for overhead: using ${intervalToUse}ms instead of configured ${refreshInterval}ms`); }
      else this.log(`Interval not adjusted (configured <= 1 hour): using ${intervalToUse}ms`);
      await autoScrollToFullBottom();
      await this.loadPreviousValues();
      await this.checkAndNotify();
      if (this.checkInterval) { clearInterval(this.checkInterval); this.checkInterval = null; }
      this.checkInterval = setInterval(async () => { this.log('Scrolling before refresh...'); await autoScrollToFullBottom(); this.log('Refreshing page...'); window.location.reload(); }, intervalToUse);
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
    this.stop();
    await autoScrollToFullBottom();
    await this.checkAndNotify();
    this.start();
  }

  // interim summary (manual request)
  async handleInterimSummaryRequest() {
    this.log('Interim summary requested');
    const currentValues = this.getCurrentValues();
    if (!currentValues) { this.error('Interim summary aborted: could not extract current values'); throw new Error('No current values'); }
    const baseline = await new Promise(res => chrome.storage.local.get([this._dailyStatsKey], r => res(r?.[this._dailyStatsKey] || null)));
    if (!baseline) { this.error('Interim summary aborted: no baseline dailyStats found'); throw new Error('No baseline dailyStats found'); }

    const previousDay = baseline;
    const modelChanges = {};
    for (const [id, current] of Object.entries(currentValues.models)) {
      let previous = previousDay?.models?.[id] || null;
      if (!previous && current.permalink) previous = Object.values(previousDay.models || {}).find(m => m && m.permalink === current.permalink) || null;
      if (!previous && current.name) previous = Object.values(previousDay.models || {}).find(m => m?.name?.trim().toLowerCase() === current.name.trim().toLowerCase()) || null;
      if (!previous) { this.log('New model found (interim):', current.name); continue; }
      const downloadsGained = Math.max(0, (Number(current.downloads) || 0) - (Number(previous.downloads) || 0));
      const printsGained = Math.max(0, (Number(current.prints) || 0) - (Number(previous.prints) || 0));
      if (downloadsGained > 0 || printsGained > 0) modelChanges[id] = { id, name: current.name, downloadsGained, printsGained, previousDownloads: previous.downloads || 0, previousPrints: previous.prints || 0, currentDownloads: current.downloads || 0, currentPrints: current.prints || 0, imageUrl: current.imageUrl || '' };
    }

    const dailyDownloads = Object.values(modelChanges).reduce((s,m)=>s+m.downloadsGained,0);
    const dailyPrints = Object.values(modelChanges).reduce((s,m)=>s+m.printsGained,0);
    const top5Downloads = Object.values(modelChanges).filter(m=>m.downloadsGained>0).sort((a,b)=>b.downloadsGained-a.downloadsGained).slice(0,5);
    const top5Prints = Object.values(modelChanges).filter(m=>m.printsGained>0).sort((a,b)=>b.printsGained-a.printsGained).slice(0,5);

    let rewardsEarned = [], rewardPointsTotal = 0;
    for (const m of Object.values(modelChanges)) {
      const prevTotal = this.calculateDownloadsEquivalent(m.previousDownloads, m.previousPrints);
      const currTotal = this.calculateDownloadsEquivalent(m.currentDownloads, m.currentPrints);
      let cursor = prevTotal; const thresholdsHit=[]; const maxThresholdsPerModel=50; let count=0;
      while (cursor < currTotal && count < maxThresholdsPerModel) {
        const interval = this.getRewardInterval(cursor), mod = cursor % interval;
        const nextThreshold = (cursor===0 || mod===0) ? cursor+interval : cursor+(interval-mod);
        if (nextThreshold <= currTotal) { const pts = this.getRewardPointsForDownloads(nextThreshold); thresholdsHit.push({ threshold: nextThreshold, points: pts }); rewardPointsTotal += pts; cursor = nextThreshold; count++; } else break;
      }
      if (thresholdsHit.length) rewardsEarned.push({ id:m.id, name:m.name, thresholds:thresholdsHit.map(t=>t.threshold), rewardPointsTotalForModel:thresholdsHit.reduce((s,t)=>s+t.points,0) });
    }

    const totalEquivalent = dailyDownloads + (dailyPrints * 2);
    const topDownloadsList = top5Downloads.length ? top5Downloads.map((m,i)=>`${i+1}. ${m.name}: +${m.downloadsGained}`).join('\n') : 'No new downloads so far';
    const topPrintsList = top5Prints.length ? top5Prints.map((m,i)=>`${i+1}. ${m.name}: +${m.printsGained}`).join('\n') : 'No new prints so far';
    const fromTs = new Date(previousDay.timestamp).toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' }), toTs = new Date().toLocaleString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric', hour: 'numeric', minute: '2-digit' });
    const message = `
🔔 Interim Summary (${fromTs} - ${toTs}):

Total Downloads: ${totalEquivalent} [downloads + 2x prints]

🏆 Top Downloads:
${topDownloadsList}

🖨️ Top Prints:
${topPrintsList}

🎁 Rewards earned so far:
${rewardsEarned.length > 0 ? rewardsEarned.map(r => `${r.name}: +${r.rewardPointsTotalForModel} points (thresholds: ${r.thresholds.join(', ')})`).join('\n') : 'No rewards earned so far'}
`.trim();
    this.log('Interim message:', message);
    const sent = await this.sendTelegramMessage(message);
    if (!sent) { this.error('Interim summary: failed to send via Telegram'); throw new Error('Telegram send failed'); }
    this.log('Interim summary: sent successfully');
    return true;
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
    chrome.storage.sync.get(['notifySummaryMode'], cfg => { monitor.notifySummaryMode = !!(cfg?.notifySummaryMode); monitor.log('CONFIG_SAVED received. notifySummaryMode =', monitor.notifySummaryMode); monitor.restart().then(()=>sendResponse({ok:true})).catch(err=>sendResponse({ok:false, error: err?.message})); });
    return true;
  }
});
