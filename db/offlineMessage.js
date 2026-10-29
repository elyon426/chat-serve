import { DataAPIClient, Db } from "@datastax/astra-db-ts";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

const endpoint = process.env.ASTRA_DB_API_ENDPOINT;
const token = process.env.ASTRA_DB_APPLICATION_TOKEN;

if (!endpoint || !token) {
  throw new Error("Missing Astra DB credentials in environment variables.");
}

// 1. Initialize the general Data API Client
const client = new DataAPIClient(token);

// 2. Connect to your specific database instance and export it
export const db = client.db(endpoint);
