import { DFAlg } from "./dataframe";
import { ExprAlg } from "./expressions";

//export type DFProgram<R, E, G = unknown, CDF = {}, CEX = {}> = (DF: DFAlg<R, E, G> & CDF, EX: ExprAlg<E> & CEX) => R;
export type DFProgram<R = unknown, E = unknown, G = unknown, CDF = unknown, CEX = unknown> =(DF: DFAlg<R, E, G, CDF>, EX: ExprAlg<E> & CEX) => R;