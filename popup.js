document.addEventListener('DOMContentLoaded', function () {
    const telegramTokenInput = document.getElementById('telegram-token');
    const chatIdInput = document.getElementById('chat-id');
    const refreshIntervalSelect = document.getElementById('refresh-interval');
    const dailyReportSelect = document.getElementById('daily-report');
    const notificationTimeInput = document.getElementById('notification-time');
    const saveButton = document.getElementById('save-button');
    const interimButton = document.getElementById('interim-button');
    const statusDiv = document.getElementById('status');

    // New summary checkbox element (ensure popup.html includes an element with this id)
    const notifySummaryCheckbox = document.getElementById('notify-summary-mode');

    function showStatus(message, isError = false) {
        statusDiv.textContent = message;
        statusDiv.className = 'status ' + (isError ? 'error' : 'success');
        setTimeout(() => {
            statusDiv.textContent = '';
            statusDiv.className = 'status';
        }, 3000);
    }

    // Load saved configuration (including notifySummaryMode)
    chrome.storage.sync.get(
        ['telegramToken', 'chatId', 'refreshInterval', 'dailyReport', 'dailyNotificationTime', 'notifySummaryMode'],
        function (config) {
            if (chrome.runtime.lastError) {
                console.error('Error loading configuration:', chrome.runtime.lastError);
                showStatus('Error loading configuration', true);
                return;
            }

            if (config.telegramToken) telegramTokenInput.value = config.telegramToken;
            if (config.chatId) chatIdInput.value = config.chatId;
            if (config.refreshInterval) refreshIntervalSelect.value = String(config.refreshInterval);
            if (config.dailyReport) dailyReportSelect.value = config.dailyReport;
            if (config.dailyNotificationTime) notificationTimeInput.value = config.dailyNotificationTime;
            notificationTimeInput.disabled = dailyReportSelect.value === 'no';

            // initialize checkbox if present in DOM
            if (notifySummaryCheckbox) {
                notifySummaryCheckbox.checked = !!config.notifySummaryMode;
            }
        }
    );

    // Save configuration
    saveButton.addEventListener('click', function () {
        const telegramToken = telegramTokenInput.value.trim();
        const chatId = chatIdInput.value.trim();
        const refreshInterval = Number(refreshIntervalSelect.value);
        const dailyReport = dailyReportSelect.value;
        const notificationTime = notificationTimeInput.value;
        const notifySummaryMode = notifySummaryCheckbox ? !!notifySummaryCheckbox.checked : false;

        if (!telegramToken || !chatId) {
            showStatus('Please fill in all fields', true);
            return;
        }

        chrome.storage.sync.set({
            telegramToken: telegramToken,
            chatId: chatId,
            refreshInterval: refreshInterval,
            dailyReport: dailyReport,
            dailyNotificationTime: notificationTime,
            notifySummaryMode: notifySummaryMode
        }, function() {
            if (chrome.runtime.lastError) {
                console.error('Error saving:', chrome.runtime.lastError);
                showStatus('Error saving configuration', true);
                return;
            }

            showStatus('Configuration saved!');

            // Notify content script in active tab to restart timers (existing behavior)
            chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
                if (tabs[0]) {
                    chrome.tabs.sendMessage(tabs[0].id, { type: 'REFRESH_INTERVAL_UPDATED' }, (resp) => {
                        if (chrome.runtime.lastError) {
                            console.warn('notify content script error', chrome.runtime.lastError.message);
                        } else {
                            console.log('Content script restart response', resp);
                        }
                    });
                    // reload page to ensure content script picks up any page load changes
                    chrome.tabs.reload(tabs[0].id);
                }
            });

            // Notify content scripts/background that config was saved and summary mode should activate only after save
            // Send a runtime message to listeners (content script listens for CONFIG_SAVED)
            chrome.runtime.sendMessage({ type: 'CONFIG_SAVED' }, (response) => {
                // ignore response; content scripts will call monitor.restart when they receive CONFIG_SAVED
            });

            // Also broadcast to all tabs (content scripts that are not active tab)
            chrome.tabs.query({}, (allTabs) => {
                for (const tab of allTabs) {
                    try {
                        chrome.tabs.sendMessage(tab.id, { type: 'CONFIG_SAVED' }, () => { /* ignore errors */ });
                    } catch (e) {
                        // some tabs may not accept messages; ignore
                    }
                }
            });
        });
    });

    // Interim summary button
    interimButton.addEventListener('click', function () {
        statusDiv.textContent = 'Requesting interim summary...';
        chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
            const tab = tabs[0];
            if (!tab) {
                statusDiv.textContent = 'No active tab.';
                return;
            }

            chrome.tabs.sendMessage(tab.id, { type: 'INTERIM_SUMMARY_REQUEST' }, (response) => {
                if (chrome.runtime.lastError) {
                    statusDiv.textContent = `Error: ${chrome.runtime.lastError.message}`;
                    return;
                }
                if (response && response.ok) {
                    statusDiv.textContent = 'Interim summary sent.';
                } else {
                    statusDiv.textContent = 'Interim summary failed to send.';
                    console.error('INTERIM_SUMMARY_RESPONSE', response);
                }
            });
        });
    });

    // Handle notification time change
    notificationTimeInput.addEventListener('change', function() {
        if (dailyReportSelect.value === 'yes') {
            chrome.storage.sync.set({
                dailyNotificationTime: notificationTimeInput.value
            }, function() {
                showStatus('Notification time updated!');
                // Inform content script to reschedule immediately
                chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
                    if (tabs[0]) {
                        chrome.tabs.sendMessage(tabs[0].id, { type: 'REFRESH_INTERVAL_UPDATED' }, () => {});
                    }
                });
            });
        }
    });

    // Handle daily report enable/disable
    dailyReportSelect.addEventListener('change', function() {
        notificationTimeInput.disabled = this.value === 'no';
        // Persist the setting immediately so content script can act quickly
        chrome.storage.sync.set({ dailyReport: this.value }, function() {
            // notify content script
            chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
                if (tabs[0]) {
                    chrome.tabs.sendMessage(tabs[0].id, { type: 'REFRESH_INTERVAL_UPDATED' }, () => {});
                }
            });
        });
    });
    // Get the new button element and add its listener
    const restartTimerButton = document.getElementById('restartTimerButton');
    restartTimerButton.addEventListener('click', function () {
        showStatus('Restarting timer...');

        // Broadcast REFRESH_INTERVAL_UPDATED to every MakerWorld tab
        chrome.tabs.query({ url: '*://makerworld.com/*' }, function (tabs) {
            if (!tabs || !tabs.length) {
                showStatus('No MakerWorld tabs found — open a MakerWorld page first.');
                return;
            }

            let successes = 0;
            for (const tab of tabs) {
                chrome.tabs.sendMessage(
                    tab.id,
                    { type: 'REFRESH_INTERVAL_UPDATED' },
                    (resp) => {
                        if (chrome.runtime.lastError) {
                            console.warn(
                                `Restart message failed for tab ${tab.id}:`,
                                chrome.runtime.lastError.message
                            );
                            return;
                        }
                        successes++;
                        console.log(
                            `Timer restart message delivered to tab ${tab.id}`
                        );
                    }
                );
            }

            showStatus(
                `Restart command sent to ${tabs.length} tab${
                    tabs.length > 1 ? 's' : ''
                }.`
            );
        });

        // Also send a runtime-wide broadcast in case a background listener exists
        chrome.runtime.sendMessage({ type: 'REFRESH_INTERVAL_UPDATED' }, () => {
            /* ignore response */
        });
    }); // ← closes restartTimerButton click handler

}); // ← closes the outer DOMContentLoaded wrapper
