import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'telegram_message_links' })
@Index(['message_link'], { unique: true })
export class TelegramMessageLinkEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'integer', nullable: true })
  @Index()
  telegram_chat_id?: number | null;

  @Column({ type: 'varchar', length: 512 })
  message_link!: string;

  @Column({ type: 'integer', nullable: true })
  message_id?: number | null;

  @Column({ type: 'varchar', length: 512, nullable: true })
  invite_link?: string | null;

  @Column({ type: 'varchar', length: 512, nullable: true })
  discovered_from_link?: string | null;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
