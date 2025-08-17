const js = require('@eslint/js');
const tsParser = require('@typescript-eslint/parser');
const tsPlugin = require('@typescript-eslint/eslint-plugin');
const globals = require('globals');

module.exports = [
    // Ignorar artefactos y generados
    { ignores: ['dist/**', 'node_modules/**', 'coverage/**', 'proto/**', 'eslint.config.*', 'docs/examples/**'] },

    // Base JS
    js.configs.recommended,

    // Reglas para TypeScript (src/**)
    {
        files: ['src/**/*.ts'],
        languageOptions: {
            parser: tsParser,
            parserOptions: { ecmaVersion: 'latest', sourceType: 'module' },
            // Entorno Node: habilita Buffer, console, etc.
            globals: globals.node,
        },
        plugins: { '@typescript-eslint': tsPlugin },
        rules: {
            // Usar solo la variante TS de unused-vars
            'no-unused-vars': 'off',
            '@typescript-eslint/no-unused-vars': [
                'warn',
                { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' },
            ],

            // Apagar “import type” obligatorio (si te gusta, cambiá a 'warn')
            '@typescript-eslint/consistent-type-imports': 'off',
        },
    },

    // Tests (test/** o tests/**)
    {
        files: ['test/**/*.{ts,tsx}', 'tests/**/*.{ts,tsx}'],
        languageOptions: {
            parser: tsParser,
            parserOptions: { ecmaVersion: 'latest', sourceType: 'module' },
            globals: {
                ...globals.node,
                describe: 'readonly',
                it: 'readonly',
                test: 'readonly',
                expect: 'readonly',
                beforeAll: 'readonly',
                beforeEach: 'readonly',
                afterAll: 'readonly',
                afterEach: 'readonly',
            },
        },
        plugins: { '@typescript-eslint': tsPlugin },
        rules: {
            'no-unused-vars': 'off',
            '@typescript-eslint/no-unused-vars': [
                'off', // en tests suele haber variables auxiliares; si querés ruido, poné 'warn'
            ],
            '@typescript-eslint/consistent-type-imports': 'off',
        },
    },
];
