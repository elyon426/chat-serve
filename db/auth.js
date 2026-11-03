import 'dotenv/config';
import crypto from 'crypto';
import { neon } from '@neondatabase/serverless';

const sql = neon(process.env.DATABASE_URL); // same Neon DB the REST server writes to

export async function authenticateToken(plainToken) {
  if (!plainToken || typeof plainToken !== 'string') return { error: 'No Token' };

  const hash = crypto.createHash('sha256').update(plainToken).digest('hex');
  try {
    const [row] = await sql`SELECT user_id FROM devices WHERE tokenhash = ${hash} LIMIT 1`;
    if (!row) return { error: 'InvalidToken' };
    return { userId: row.user_id };
  } catch (error) {
    console.error('Database authentication error:', error);
    return { error: 'InternalServerError' };
  }
}
