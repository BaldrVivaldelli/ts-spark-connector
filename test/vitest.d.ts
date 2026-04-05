declare module "vitest" {
  export function describe(name: string, fn: () => void): void;
  export function it(name: string, fn: () => void | Promise<void>, timeout?: number): void;
  export function beforeAll(fn: () => void | Promise<void>, timeout?: number): void;

  export type Matchers = {
    [key: string]: (...args: unknown[]) => void;
    toBe(expected: unknown): void;
    toEqual(expected: unknown): void;
    toBeTruthy(): void;
    toBeFalsy(): void;
    toMatch(expected: RegExp | string): void;
    toThrow(expected?: RegExp | string): void;
    toBeGreaterThan(expected: number): void;
    toBeUndefined(): void;
  };

  export function expect<T = unknown>(actual: T): Matchers;
}
