// Connect the warm Telegram client (login + realtime updates). This is the
// readiness gate for operations: it does NOT seed the dialog archive, so it
// completes in a few seconds even on a large account. A single in-flight promise
// is shared so concurrent callers (the ensureLogin hook each control handler
// runs) await one connect rather than starting several.
export function createTelegramConnector({ telegramClient, messageSyncService }) {
  let telegramReady = false;
  let telegramConnectPromise = null;

  async function seedArchiveInBackground() {
    try {
      const dialogCount = await messageSyncService.refreshChannelsFromDialogs();
      console.log(`[startup] Seeded ${dialogCount} dialogs into archive registry.`);
      messageSyncService.resumePendingJobs();
    } catch (error) {
      console.error(`[startup] Background archive seeding failed: ${error?.message ?? error}`);
    }
  }

  return function ensureTelegramConnected() {
    if (telegramReady) {
      return Promise.resolve();
    }
    if (!telegramConnectPromise) {
      telegramConnectPromise = (async () => {
        // Subscribe before logging in: mtcute starts the updates loop inside
        // client.start() and persists the new pts once it has dispatched the missed
        // updates, so whatever it dispatches to nobody is gone for good.
        messageSyncService.startRealtimeSync();
        console.log('[startup] Connecting Telegram client...');
        const dialogsReady = await telegramClient.initializeDialogCache();
        if (!dialogsReady) {
          throw new Error('Failed to initialize Telegram dialog list');
        }
        telegramReady = true;
        console.log('[startup] Telegram client connected.');
        // Seed the archive registry and start queue work in the background; ops do
        // their own live fetches and must not wait on the full dialog refresh, which
        // is slow on large accounts.
        void seedArchiveInBackground();
      })().catch((error) => {
        // Reset so a later operation can retry the connect instead of being stuck
        // on a permanently rejected promise.
        telegramConnectPromise = null;
        throw error;
      });
    }
    return telegramConnectPromise;
  };
}
