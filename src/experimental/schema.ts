/**
 * EXPERIMENTAL — re-exporta la declaración de schema ya promovida.
 *
 * La implementación de `schema()`/`DeclaredSchema` (y sus tipos token→tipo /
 * token→DDL) fue promovida al módulo público `src/schema/schema.ts`
 * (Requirement 4.6). Este archivo se conserva como un re-export delgado para
 * no romper los imports existentes (`src/experimental/typed-dataframe.ts` y la
 * batería de tests del prototipo); la reubicación/eliminación final la maneja
 * la tarea 10.1.
 */
export * from "../schema/schema";
