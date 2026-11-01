import crypto from 'crypto'; 
import { db } from './offlineMessage.js'; 

export async function authenticateToken(plainToken) { 
  if (!plainToken || typeof plainToken !== 'string') {
    return { error: 'No Token' }; 
  }

  // Hash token securely
  const hashToken = crypto.createHash('sha256').update(plainToken).digest('hex'); 

  try {
    // Note: ensure your DB library returns [rows] as an array
    const [rows] = await db.query(
      'SELECT userId FROM tokenhash WHERE tokenhash = ? AND is_valid = 1 LIMIT 1',
      [hashToken]
    ); 

    if (!rows || rows.length === 0) { 
      return { error: 'InvalidToken' }; 
    } 

    const { userId } = rows[0]; 
    return { userId }; 

  } catch (error) {
    // Log the actual error internally, but don't expose it to the client
    console.error('Database authentication error:', error);
    return { error: 'InternalServerError' };
  }
}
