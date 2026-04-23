/**
 * MCPL (MCP Live) Protocol Types for Zulip MCP Server
 *
 * JSON-RPC 2.0 transport types and MCPL-specific structures matching
 * the agent-framework's types.ts wire format.
 */

// ============================================================================
// JSON-RPC 2.0 Transport
// ============================================================================

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  method: string;
  id?: string | number;
  params?: Record<string, unknown>;
}

export interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: string | number;
  result?: unknown;
  error?: JsonRpcError;
}

export interface JsonRpcError {
  code: number;
  message: string;
  data?: unknown;
}

// ============================================================================
// MCPL Content Blocks
// ============================================================================

export type McplContentBlock = McplTextContent | McplImageContent;

export interface McplTextContent {
  type: 'text';
  text: string;
}

export interface McplImageContent {
  type: 'image';
  data?: string;
  mimeType?: string;
  uri?: string;
}

// ============================================================================
// MCPL Capabilities
// ============================================================================

export interface McplServerCapabilities {
  version: string;
  pushEvents?: boolean;
  contextHooks?: {
    beforeInference?: boolean;
    afterInference?: boolean;
  };
  featureSets?: Record<string, FeatureSetDeclaration>;
  channels?: McplChannelCapabilities;
}

export interface McplChannelCapabilities {
  register?: boolean;
  publish?: boolean;
  observe?: boolean;
  lifecycle?: boolean;
}

// ============================================================================
// Feature Sets
// ============================================================================

export type FeatureSetUse =
  | 'pushEvents'
  | 'contextHooks.beforeInference'
  | 'contextHooks.afterInference'
  | 'tools'
  | 'channels.publish'
  | 'channels.observe';

export interface FeatureSetDeclaration {
  description: string;
  uses: FeatureSetUse[];
}

export interface FeatureSetsUpdateParams {
  enabled?: string[];
  disabled?: string[];
}

// ============================================================================
// Channels
// ============================================================================

export interface ChannelDescriptor {
  id: string;
  type: string;
  label: string;
  direction: 'outbound' | 'inbound' | 'bidirectional';
  address?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export interface ChannelIncomingMessage {
  channelId: string;
  messageId: string;
  threadId?: string;
  author: { id: string; name: string };
  timestamp: string;
  content: McplContentBlock[];
  metadata?: Record<string, unknown>;
}

export interface ChannelsRegisterParams {
  channels: ChannelDescriptor[];
}

export interface ChannelsIncomingParams {
  messages: ChannelIncomingMessage[];
}

export interface ChannelsPublishParams {
  conversationId: string;
  channelId: string;
  content: McplContentBlock[];
}

export interface ChannelsListResult {
  channels: ChannelDescriptor[];
}

export interface ChannelsOpenParams {
  type: string;
  address?: Record<string, unknown>;
}

export interface ChannelsCloseParams {
  channelId: string;
}

// ============================================================================
// Context Hooks
// ============================================================================

export interface McplContextInjection {
  namespace: string;
  position: 'system' | 'beforeUser' | 'afterUser';
  content: string | McplContentBlock[];
  metadata?: Record<string, unknown>;
}

export interface BeforeInferenceParams {
  inferenceId: string;
  conversationId: string;
  turnIndex: number;
  userMessage: string | null;
  model: { id: string; vendor: string; contextWindow: number; capabilities: string[] };
}

export interface BeforeInferenceResult {
  featureSet: string;
  contextInjections: McplContextInjection[];
}

export interface AfterInferenceParams {
  inferenceId: string;
  conversationId: string;
  turnIndex: number;
  userMessage: string | null;
  assistantMessage: string;
  model: { id: string; vendor: string; contextWindow: number; capabilities: string[] };
  usage: { inputTokens: number; outputTokens: number };
}

// ============================================================================
// MCPL Method Names
// ============================================================================

export const McplMethod = {
  PushEvent: 'push/event',
  BeforeInference: 'context/beforeInference',
  AfterInference: 'context/afterInference',
  FeatureSetsUpdate: 'featureSets/update',
  FeatureSetsChanged: 'featureSets/changed',
  ChannelsRegister: 'channels/register',
  ChannelsChanged: 'channels/changed',
  ChannelsList: 'channels/list',
  ChannelsOpen: 'channels/open',
  ChannelsClose: 'channels/close',
  ChannelsPublish: 'channels/publish',
  ChannelsIncoming: 'channels/incoming',
  ChannelsTyping: 'channels/typing',
} as const;

export type McplMethodName = (typeof McplMethod)[keyof typeof McplMethod];

/** Set of all MCPL method strings for routing. */
export const MCPL_METHODS = new Set<string>(Object.values(McplMethod));
