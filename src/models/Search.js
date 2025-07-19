const mongoose = require('mongoose');
const { Schema } = mongoose;

// A single message hit
const MessageSchema = new Schema({
  text:     { type: String, required: true },
  url:      { type: String, required: true },
  found_at: { type: Date,   required: true }
}, { _id: false });

// One chat (channel|group|bot) with its hits
const TelegramChatSchema = new Schema({
  category: { type: String, enum: ['channel','group','bot'], required: true },
  username: { type: String, required: true },   // must match what service provides
  title:    { type: String, required: true },
  messages: [ MessageSchema ]
}, { _id: false });

const WebResultSchema = new Schema({
  title:    String,
  url:      String,
  found_at: Date
}, { _id: false });

const SearchSchema = new Schema({
  user_id:    { type: Schema.Types.ObjectId, ref: 'User', required: true },
  keyword:    { type: String, required: true },
  results: {
    web:      [ WebResultSchema ],
    telegram: [ TelegramChatSchema ]
  },
  created_at: { type: Date, default: () => new Date() }
});

// index so you can quickly fetch the latest
SearchSchema.index({ user_id: 1, keyword: 1, created_at: -1 });

module.exports = mongoose.model('Search', SearchSchema);