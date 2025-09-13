import { DFAlg } from "./dataframe";
import { ExprAlg } from "./expressions";


export type DFProgram<R, E, G = unknown> = (DF: DFAlg<R, E, G>, EX: ExprAlg<E>) => R;