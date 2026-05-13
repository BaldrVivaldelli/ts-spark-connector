declare const __dirname: string;
declare const process: {
  env: Record<string, string | undefined>;
};

type Buffer = any;
declare const Buffer: {
  from(input: string | ArrayBuffer | Uint8Array, encoding?: string): any;
};

declare module "crypto" {
  const crypto: {
    randomUUID(): string;
  };
  export default crypto;
}

declare module "node:fs" {
  const fs: {
    existsSync(path: string): boolean;
    readFileSync(path: string): any;
  };
  export default fs;
}

declare module "node:path" {
  const path: {
    resolve(...segments: string[]): string;
    join(...segments: string[]): string;
  };
  export default path;
}

declare module "node:os" {
  const os: {
    userInfo(): { username: string };
  };
  export default os;
}

declare module "@grpc/grpc-js" {
  export class Metadata {
    set(key: string, value: string): void;
  }

  export namespace credentials {
    function createInsecure(): any;
    function createSsl(rootCerts?: any, privateKey?: any, certChain?: any): any;
  }

  export type ChannelCredentials = any;
  export type ChannelOptions = Record<string, any>;

  export function loadPackageDefinition(definition: any): any;
}

declare module "@grpc/proto-loader" {
  export function loadSync(files: string[], options?: any): any;
}

declare module "apache-arrow" {
  export function tableFromIPC(data: any): any;
}
