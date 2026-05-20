import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

export interface WebResult {
  title: string;
  url: string;
  found_at: Date;
}

export interface TelegramMessage {
  text: string;
  url: string;
  found_at: Date;
}

export interface TelegramChat {
  category: 'channel' | 'group' | 'bot';
  username: string;
  title: string;
  messages: TelegramMessage[];
}

@Entity({ name: 'searches' })
@Index(['user_id', 'keyword', 'created_at'])
export class SearchEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'integer' })
  user_id!: number;

  @Column({ type: 'varchar', length: 256 })
  keyword!: string;

  @Column({ type: 'simple-json', nullable: true })
  results_web?: WebResult[];

  @Column({ type: 'simple-json', nullable: true })
  results_telegram?: TelegramChat[];

  @Column({ name: 'RESULTS_LINKS', type: 'text', nullable: true })
  results_links?: string;

  @Column({ name: 'RESULTS_INVITES', type: 'text', nullable: true })
  results_invites?: string;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
