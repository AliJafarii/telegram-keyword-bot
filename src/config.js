require('dotenv').config();

module.exports = {
  botToken:      process.env.TELEGRAM_BOT_TOKEN,
  mongoUri:      process.env.MONGO_URI,
  // wrap in Number() or parseInt
  apiId:         Number(process.env.API_ID),
  apiHash:       process.env.API_HASH,
  sessionString: process.env.SESSION_STRING || '',
  logLevel:      process.env.LOG_LEVEL || 'info',
};