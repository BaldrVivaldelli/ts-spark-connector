import { DataFrameReader } from "./reader";

export class SparkSession {
    read = new DataFrameReader();
}

export const spark = new SparkSession();
