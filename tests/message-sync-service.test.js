import { describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';

describe('realtime archiving', () => {
  it('does not archive a scheduled message until it is delivered', () => {
    const service = Object.create(MessageSyncService.prototype);
    service._ensureChannelFromPeer = vi.fn(() => ({ sync_enabled: 0 }));

    service._handleIncomingMessage({ chat: { id: 1 }, isScheduled: true }, { isEdit: false });
    expect(service._ensureChannelFromPeer).not.toHaveBeenCalled();

    service._handleIncomingMessage({ chat: { id: 1 }, isScheduled: false }, { isEdit: false });
    expect(service._ensureChannelFromPeer).toHaveBeenCalledTimes(1);
  });
});
