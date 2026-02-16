import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'search_links' })
@Index(['search_id', 'link'], { unique: true })
export class SearchLinkEntity {
  @PrimaryGeneratedColumn({ type: 'number' })
  id!: number;

  @Column({ type: 'number' })
  @Index()
  search_id!: number;

  @Column({ type: 'number', nullable: true })
  @Index()
  channel_match_id?: number | null;

  @Column({ type: 'varchar2', length: 512 })
  link!: string;

  @Column({ type: 'varchar2', length: 32 })
  link_type!: string;

  @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
