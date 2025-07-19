#!/usr/bin/env node
require('dotenv').config();
require('../src/db');          // ensures MongoDB connection & indexes
const { crawlAll } = require('../src/services/crawlerService');

crawlAll();