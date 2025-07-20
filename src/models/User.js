// src/models/User.js
const mongoose = require('mongoose');

const userSchema = new mongoose.Schema({
  telegram_id:  { type: String, required: true, unique: true },
  first_name:   { type: String },
  last_name:    { type: String },
  username:     { type: String },
  language_code:{ type: String },
  phone_number: { type: String },
}, {
  timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

module.exports = mongoose.model('User', userSchema);