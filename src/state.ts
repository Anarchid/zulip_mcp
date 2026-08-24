/**
 * Checkpoint tracking for zulip.messaging rollback (SPEC §8, out of scope
 * for 0.5.0 but still handled, as discord-mcpl does).
 *
 * Every message this server sends — channels/publish and the send tools —
 * is recorded. A checkpoint marks a point in that record; `state/rollback`
 * to a checkpoint hands back the messages sent after it so the caller can
 * delete them (best-effort: Zulip lets a bot delete its own messages).
 */

import { randomUUID } from 'node:crypto';

export interface SentMessage {
  messageId: string;
  channelId: string;
  content: string;
  timestamp: string;
}

export interface Checkpoint {
  id: string;
  parent: string | null;
  /** Index into the sent-messages record — everything at this index and after was sent post-checkpoint. */
  sentCount: number;
}

export class StateTracker {
  private sentMessages: SentMessage[] = [];
  private checkpoints = new Map<string, Checkpoint>();
  private currentCheckpoint: string | null = null;

  /** Record a message this server sent (so it can be undone on rollback). */
  recordSent(messageId: string, channelId: string, content: string): void {
    this.sentMessages.push({ messageId, channelId, content, timestamp: new Date().toISOString() });
  }

  /** Create a new checkpoint at the current point in the record. */
  createCheckpoint(): string {
    const id = `chk_${randomUUID().slice(0, 8)}`;
    this.checkpoints.set(id, { id, parent: this.currentCheckpoint, sentCount: this.sentMessages.length });
    this.currentCheckpoint = id;
    return id;
  }

  get current(): string | null {
    return this.currentCheckpoint;
  }

  /**
   * Roll back to a checkpoint: returns the messages sent after it (to be
   * deleted) and truncates the record to it. Null when the checkpoint is
   * unknown. Old checkpoints are kept — a later rollback may target them.
   */
  rollback(checkpointId: string): SentMessage[] | null {
    const checkpoint = this.checkpoints.get(checkpointId);
    if (!checkpoint) return null;
    const toDelete = this.sentMessages.slice(checkpoint.sentCount);
    this.sentMessages = this.sentMessages.slice(0, checkpoint.sentCount);
    this.currentCheckpoint = checkpointId;
    return toDelete;
  }

  /** The current checkpoint as tool results report it. */
  getCheckpointState(): { checkpoint: string; parent: string | null } | null {
    if (!this.currentCheckpoint) return null;
    const cp = this.checkpoints.get(this.currentCheckpoint);
    return cp ? { checkpoint: cp.id, parent: cp.parent } : null;
  }
}
