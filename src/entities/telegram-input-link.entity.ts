import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'telegram_input_links' })
@Index(['link'], { unique: true })
export class TelegramInputLinkEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'varchar', length: 512 })
  link!: string;

  @Column({ type: 'varchar', length: 32 })
  link_type!: string;

  @Column({ type: 'integer', nullable: true })
  telegram_chat_id?: number | null;

  @Column({ type: 'varchar', length: 32 })
  status!: string;

  @Column({ type: 'varchar', length: 256, nullable: true })
  source_label?: string | null;

  @Column({ type: 'text', nullable: true })
  last_error?: string | null;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;

  @Column({ default: () => 'CURRENT_TIMESTAMP' })
  updated_at!: Date;
}
