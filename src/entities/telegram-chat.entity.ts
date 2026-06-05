import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'telegram_chats' })
@Index(['chat_key'], { unique: true })
@Index(['invite_link'], { unique: true })
export class TelegramChatEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'varchar', length: 128 })
  chat_key!: string;

  @Column({ type: 'varchar', length: 32 })
  chat_type!: string;

  @Column({ type: 'varchar', length: 512, nullable: true })
  title?: string | null;

  @Column({ type: 'varchar', length: 128, nullable: true })
  username?: string | null;

  @Column({ type: 'varchar', length: 512, nullable: true })
  public_link?: string | null;

  @Column({ type: 'varchar', length: 512, nullable: true })
  invite_link?: string | null;

  @Column({ type: 'varchar', length: 64, nullable: true })
  private_uid?: string | null;

  @Column({ type: 'varchar', length: 32 })
  join_status!: string;

  @Column({ type: 'text', nullable: true })
  last_error?: string | null;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  updated_at!: Date;
}
