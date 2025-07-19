// src/models/ChannelMatch.js
const mongoose = require('mongoose');
const { Schema } = mongoose;

const ChannelMatchSchema = new Schema({
  channel:    String, // e.g. "netflixseriesupdates"
  message_id: Number, // the Telegram message ID
  date:       Date,
  text:       String  // full message text
});

// create a text index on `text` so we can $text-search quickly
ChannelMatchSchema.index({ text: 'text' });

module.exports = mongoose.model('ChannelMatch', ChannelMatchSchema);