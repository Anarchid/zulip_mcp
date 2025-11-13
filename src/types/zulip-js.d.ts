declare module 'zulip-js' {
  interface ZulipConfig {
    username?: string;
    apiKey?: string;
    password?: string;
    realm?: string;
    zuliprc?: string;
  }

  interface ZulipClient {
    messages: {
      send: (params: any) => Promise<any>;
      retrieve: (params: any) => Promise<any>;
      update: (params: any) => Promise<any>;
      deleteById: (params: { message_id: number }) => Promise<any>;
      getHistoryById: (params: { message_id: number }) => Promise<any>;
      render: (params: { content: string }) => Promise<any>;
    };
    streams: {
      retrieve: (params?: any) => Promise<any>;
      getStreamId: (params: { stream: string }) => Promise<any>;
      subscriptions: {
        retrieve: (params?: any) => Promise<any>;
      };
      deleteById: (params: { stream_id: number }) => Promise<any>;
      topics: {
        retrieve: (params: { stream_id: number }) => Promise<any>;
      };
    };
    users: {
      retrieve: (params?: any) => Promise<any>;
      create: (params: any) => Promise<any>;
      me: {
        getProfile: () => Promise<any>;
        pointer: {
          retrieve: () => Promise<any>;
          update: (params: { pointer: number }) => Promise<any>;
        };
        subscriptions: (params: any) => Promise<any>;
        alertWords: {
          retrieve: () => Promise<any>;
        };
      };
    };
    reactions: {
      add: (params: any) => Promise<any>;
      remove: (params: any) => Promise<any>;
    };
    typing: {
      send: (params: { to: string | string[]; op: 'start' | 'stop' }) => Promise<any>;
    };
    events: {
      retrieve: (params: any) => Promise<any>;
    };
    queues: {
      register: (params: any) => Promise<any>;
      deregister: (params: { queue_id: string }) => Promise<any>;
    };
    emojis: {
      retrieve: () => Promise<any>;
    };
    server: {
      settings: () => Promise<any>;
    };
  }

  function zulipInit(config: ZulipConfig): Promise<ZulipClient>;
  export default zulipInit;
}




