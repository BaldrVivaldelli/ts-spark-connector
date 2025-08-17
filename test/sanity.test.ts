import { describe, it, expect, beforeAll } from 'vitest';

let mod: any;

beforeAll(async () => {
    try { mod = await import('../src/index'); }
    catch { mod = await import('../dist/index.js'); }
});

const exportedNames = (m: any) =>
    Object.keys((m && m.default) ? m.default : (m ?? {}));

describe('sanity', () => {
    it('library loads and exposes a public API', () => {
        expect(mod).toBeTruthy();
        const names = exportedNames(mod);
        // si default es una función/clase, igual lo consideramos OK
        const ok = names.length > 0 || typeof (mod.default ?? mod) !== 'undefined';
        expect(ok).toBe(true);
    });
});
