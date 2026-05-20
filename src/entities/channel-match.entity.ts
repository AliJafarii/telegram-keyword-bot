import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'channel_matches' })
@Index(['search_id', 'channel', 'message_id'], { unique: true })
export class ChannelMatchEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'integer', nullable: true })
  @Index()
  search_id?: number | null;

  @Column({ type: 'varchar', length: 128, nullable: true })
  channel?: string;

  @Column({ type: 'varchar', length: 16, nullable: true })
  channel_type?: string;

  @Column({ type: 'varchar', length: 512, nullable: true })
  channel_link?: string;

  @Column({ type: 'varchar', length: 512, nullable: true })
  message_link?: string;

  @Column({ type: 'integer', nullable: true })
  message_id?: number;

  @Column({ nullable: true })
  date?: Date;

  @Column({ type: 'varchar', length: 32, nullable: true })
  match_reason?: string;

  @Column({ type: 'integer', nullable: true })
  iteration_no?: number;

  @Column({ type: 'varchar', length: 512, nullable: true })
  discovered_via_link?: string;

  @Column({ type: 'varchar', length: 512, nullable: true })
  discovered_from_message_link?: string;

  @Column({ type: 'varchar', length: 128, nullable: true })
  discovered_from_channel?: string;

  @Column({ type: 'text', nullable: true })
  @Index()
  text?: string;

  @Column({ name: 'LINKS', type: 'varchar', length: 4000, nullable: true })
  links?: string;

  @Column({ type: 'text', nullable: true })
  media_metadata?: string;

  @Column({ type: 'varchar', length: 256, nullable: true })
  metadata_query?: string;
}
