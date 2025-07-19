// src/db.js
const mongoose = require('mongoose');
const logger   = require('./logger');

mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
})
  .then(async () => {
    logger.info('MongoDB connected');
    // Ensure text index on channel_matches
    const ChannelMatch = require('./models/ChannelMatch');
    await ChannelMatch.createIndexes();
  })
  .catch(err => {
    logger.error('MongoDB connection error', { error: err.message });
    process.exit(1);
  });