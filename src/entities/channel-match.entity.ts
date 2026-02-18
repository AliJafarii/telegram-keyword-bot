import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'channel_matches' })
@Index(['search_id', 'channel', 'message_id'], { unique: true })
export class ChannelMatchEntity {
  @PrimaryGeneratedColumn({ type: 'number' })
  id!: number;

  @Column({ type: 'number', nullable: true })
  @Index()
  search_id?: number | null;

  @Column({ type: 'varchar2', length: 128, nullable: true })
  channel?: string;

  @Column({ type: 'varchar2', length: 16, nullable: true })
  channel_type?: string;

  @Column({ type: 'varchar2', length: 512, nullable: true })
  channel_link?: string;

  @Column({ type: 'varchar2', length: 512, nullable: true })
  message_link?: string;

  @Column({ type: 'number', nullable: true })
  message_id?: number;

  @Column({ type: 'timestamp', nullable: true })
  date?: Date;

  @Column({ type: 'varchar2', length: 32, nullable: true })
  match_reason?: string;

  @Column({ type: 'number', nullable: true })
  iteration_no?: number;

  @Column({ type: 'varchar2', length: 512, nullable: true })
  discovered_via_link?: string;

  @Column({ type: 'varchar2', length: 512, nullable: true })
  discovered_from_message_link?: string;

  @Column({ type: 'varchar2', length: 128, nullable: true })
  discovered_from_channel?: string;

  @Column({ type: 'clob', nullable: true })
  @Index()
  text?: string;

  @Column({ name: 'LINKS', type: 'varchar2', length: 4000, nullable: true })
  links?: string;
}
