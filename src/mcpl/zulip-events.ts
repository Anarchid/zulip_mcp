/**
 * Zulip Event Loop — Real-time message delivery via long-polling.
 *
 * Registers an event queue for message events, polls for new messages,
 * and routes them through a callback. Handles queue expiry recovery
 * and graceful shutdown.
 */

export interface ZulipEventMessage {
  id: number;
  sender_id: number;
  sender_full_name: string;
  sender_email: string;
  display_recipient: string | { email: string; full_name: string; id: number }[];
  subject: string;
  content: string;
  timestamp: number;
  type: string;
}

export type OnZulipMessage = (streamName: string, message: ZulipEventMessage) => void;

export class ZulipEventLoop {
  private stopped = false;
  private queueId: string | null = null;

  /**
   * Start the long-polling event loop.
   * This method runs indefinitely until stop() is called.
   */
  async start(zulipClient: any, onMessage: OnZulipMessage): Promise<void> {
    while (!this.stopped) {
      try {
        await this.pollLoop(zulipClient, onMessage);
      } catch (error) {
        if (this.stopped) return;
        console.error('Zulip event loop error, restarting in 5s:', error);
        await this.sleep(5000);
      }
    }
  }

  /**
   * Stop the event loop gracefully.
   */
  stop(): void {
    this.stopped = true;
  }

  private async pollLoop(zulipClient: any, onMessage: OnZulipMessage): Promise<void> {
    // Register event queue
    const registration = await zulipClient.queues.register({
      event_types: JSON.stringify(['message']),
      all_public_streams: true,
      apply_markdown: false,
    });

    this.queueId = registration.queue_id;
    let lastEventId = registration.last_event_id;

    console.error(`Zulip event queue registered: ${this.queueId}`);

    while (!this.stopped) {
      try {
        const response = await zulipClient.events.retrieve({
          queue_id: this.queueId,
          last_event_id: lastEventId,
        });

        if (!response.events) continue;

        for (const event of response.events) {
          lastEventId = event.id;

          if (event.type === 'message' && event.message) {
            const msg = event.message as ZulipEventMessage;
            // display_recipient is a string for stream messages, array for DMs
            const streamName = typeof msg.display_recipient === 'string'
              ? msg.display_recipient
              : null;

            if (streamName && msg.type === 'stream') {
              onMessage(streamName, msg);
            }
          }
        }
      } catch (error: any) {
        if (this.stopped) return;

        // Queue not found — re-register
        const errMsg = error?.message || String(error);
        if (errMsg.includes('BAD_EVENT_QUEUE_ID') || errMsg.includes('queue_id')) {
          console.error('Zulip event queue expired, re-registering...');
          return; // Exit inner loop to re-register in outer loop
        }

        // Other error — wait and retry
        console.error('Zulip event poll error:', error);
        await this.sleep(2000);
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
