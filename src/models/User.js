const { Schema, model } = require('mongoose');

const userSchema = new Schema({
  telegram_id: { type: String, unique: true, required: true },
  username: String,
  created_at: { type: Date, default: Date.now }
});

module.exports = model('User', userSchema);