import 'reflect-metadata';
import { MongoClient } from 'mongodb';
import { AppDataSource } from './database/data-source';
import { UserEntity } from './entities/user.entity';
import { SearchEntity } from './entities/search.entity';
import { ChannelMatchEntity } from './entities/channel-match.entity';

const MONGO_URI = process.env.MONGO_URI || '';

async function migrate() {
  if (!MONGO_URI) {
    throw new Error('MONGO_URI is required for migration.');
  }

  await AppDataSource.initialize();

  const mongo = new MongoClient(MONGO_URI);
  await mongo.connect();
  const db = mongo.db();

  const usersCol = db.collection('users');
  const searchesCol = db.collection('searches');
  const channelMatchesCol = db.collection('channelmatches');

  const userRepo = AppDataSource.getRepository(UserEntity);
  const searchRepo = AppDataSource.getRepository(SearchEntity);
  const channelRepo = AppDataSource.getRepository(ChannelMatchEntity);

  const userMap = new Map<string, number>();

  const users = await usersCol.find({}).toArray();
  for (const user of users) {
    const saved = await userRepo.save({
      telegram_id: String(user.telegram_id),
      username: user.username || null,
      created_at: user.created_at ? new Date(user.created_at) : new Date()
    });
    userMap.set(String(user._id), saved.id);
  }

  const searches = await searchesCol.find({}).toArray();
  for (const search of searches) {
    const userId = userMap.get(String(search.user_id));
    if (!userId) continue;

    await searchRepo.save({
      user_id: userId,
      keyword: search.keyword || '',
      results_web: search.results?.web || [],
      results_telegram: search.results?.telegram || [],
      created_at: search.created_at ? new Date(search.created_at) : new Date()
    });
  }

  const channelMatches = await channelMatchesCol.find({}).toArray();
  for (const match of channelMatches) {
    await channelRepo.save({
      channel: match.channel ?? undefined,
      message_id: typeof match.message_id === 'number' ? match.message_id : undefined,
      date: match.date ? new Date(match.date) : undefined,
      text: match.text ?? undefined
    });
  }

  await mongo.close();
  await AppDataSource.destroy();
  // eslint-disable-next-line no-console
  console.log('Migration completed.');
}

migrate().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Migration failed', err);
  process.exit(1);
});
