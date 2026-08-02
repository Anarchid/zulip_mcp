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

/**
 * The server manifest — the `experimental.mcpl` object presented at initialize
 * (SPEC 0.5 §5.1) and returned verbatim by `mcpl/manifest` (§17.4).
 *
 * Advertisement mirrors the capability paths of §6.2: a capability with
 * sub-capabilities is a nested object whose members are the leaves of that
 * vocabulary (§5.1). A boolean `true` at an interior node is shorthand for
 * "every leaf beneath"; this server never uses that shorthand, because it
 * would over-advertise leaves it does not implement.
 */
export interface McplServerCapabilities {
  version: string;
  /** Canonical content digest of this manifest, §17.2. */
  revision?: string;
  pushEvents?: boolean;
  contextHooks?: {
    beforeInference?: McplBeforeInferenceCapability;
  };
  /** §10.5 — metadata-only replacement for the removed context/afterInference. */
  inferenceLifecycle?: boolean;
  featureSets?: Record<string, FeatureSetDeclaration>;
  channels?: McplChannelCapabilities;
}

/** §5.1 / §6.2 — observation and injection are independently granted (§10.1). */
export interface McplBeforeInferenceCapability {
  observe?: boolean;
  inject?: {
    system?: boolean;
    beforeUser?: boolean;
    afterUser?: boolean;
  };
}

/** §14.1 — channel sub-capabilities. `observe` was struck in 0.5.0. */
export interface McplChannelCapabilities {
  register?: boolean;
  lifecycle?: boolean;
  publish?: boolean;
  incoming?: boolean;
  streaming?: boolean;
  acknowledge?: boolean;
  typing?: boolean;
}

// ============================================================================
// Feature Sets
// ============================================================================

/**
 * SPEC §6.2 / App. B.2 — the closed capability-path vocabulary. `uses` MUST
 * contain only these values; a feature set whose `uses` is absent, empty, or
 * carries an unrecognized value is invalid and the host disables it with
 * reason `invalid_uses` (§6.4).
 */
export const CAPABILITY_PATHS = [
  'pushEvents',
  'tools',
  'modelInfo',
  'inferenceRequest',
  'inferenceRequest.streaming',
  'inferenceLifecycle',
  'contextHooks.beforeInference.observe',
  'contextHooks.beforeInference.inject.system',
  'contextHooks.beforeInference.inject.beforeUser',
  'contextHooks.beforeInference.inject.afterUser',
  'channels.register',
  'channels.lifecycle',
  'channels.publish',
  'channels.incoming',
  'channels.streaming',
  'channels.acknowledge',
  'channels.typing',
] as const;

export type FeatureSetUse = (typeof CAPABILITY_PATHS)[number];

export const CAPABILITY_PATH_SET: ReadonlySet<string> = new Set(CAPABILITY_PATHS);

export interface FeatureSetDeclaration {
  description: string;
  uses: FeatureSetUse[];
}

/**
 * SPEC §5.3 / §6.7 — the effective capability grant.
 *
 * `effectiveCapabilities` is the sole normative allowlist; every path not
 * present is denied. `deniedCapabilities` is derived diagnostic data only and
 * MUST NOT participate in any authorization decision.
 */
export interface FeatureSetsUpdateParams {
  effectiveCapabilities?: string[];
  deniedCapabilities?: string[];
  enabled?: string[];
  disabled?: string[];
}

/** SPEC §6.7 — the response to `featureSets/update` is a degradation receipt. */
export interface DegradationReceipt {
  accepted: true;
  mode: 'full' | 'degraded';
  unavailableFeatures: UnavailableFeature[];
  notes: string[];
}

export interface UnavailableFeature {
  featureSet: string;
  missingCapabilities: string[];
  effect: 'disabled';
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

/**
 * SPEC §14.5 — `channels/register` and `channels/changed` are authorized per
 * descriptor, and the Request form returns one entry per submitted descriptor.
 * A server MUST NOT treat a rejected descriptor as registered.
 */
export interface ChannelItemResult {
  id: string;
  accepted: boolean;
  reason?: string;
}

export interface ChannelsRegisterResult {
  results?: ChannelItemResult[];
  /** Pre-0.5 hosts answered with a flat list of accepted ids. */
  registered?: string[];
}

export interface ChannelsChangedParams {
  added?: ChannelDescriptor[];
  removed?: string[];
  updated?: ChannelDescriptor[];
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

// ============================================================================
// Server Manifest (§17)
// ============================================================================

/** SPEC §17.1 / App. B.4 — ChangeDomain. */
export type ManifestChangeDomain = 'capabilities' | 'featureSets' | 'tagOntology';

export interface ManifestChangedParams {
  revision: string;
  domains: ManifestChangeDomain[];
}

// ============================================================================
// MCPL Method Names
// ============================================================================

/**
 * `context/afterInference` and `featureSets/changed` are removed in 0.5.0
 * (§10.5, §6.7) and are deliberately absent here: an unknown method reaching
 * the dispatcher gets -32601 rather than a silent no-op result.
 */
export const McplMethod = {
  PushEvent: 'push/event',
  BeforeInference: 'context/beforeInference',
  InferenceLifecycle: 'inference/lifecycle',
  FeatureSetsUpdate: 'featureSets/update',
  ChannelsRegister: 'channels/register',
  ChannelsChanged: 'channels/changed',
  ChannelsList: 'channels/list',
  ChannelsOpen: 'channels/open',
  ChannelsClose: 'channels/close',
  ChannelsPublish: 'channels/publish',
  ChannelsIncoming: 'channels/incoming',
  ChannelsTyping: 'channels/typing',
  ChannelsAcknowledge: 'channels/acknowledge',
  ManifestChanged: 'mcpl/manifestChanged',
  Manifest: 'mcpl/manifest',
} as const;

export type McplMethodName = (typeof McplMethod)[keyof typeof McplMethod];

/** Set of all MCPL method strings for routing. */
export const MCPL_METHODS = new Set<string>(Object.values(McplMethod));
