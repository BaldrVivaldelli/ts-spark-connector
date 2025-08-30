// src/types/security.ts

export type TLSConfig = {
    keyStorePath: string;
    keyStorePassword: string;
    trustStorePath?: string;
    trustStorePassword?: string;
};

export type AuthConfig =
    | { type: "basic"; username: string; password: string }
    | { type: "token"; token: string };
