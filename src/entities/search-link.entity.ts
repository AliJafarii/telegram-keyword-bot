import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'search_links' })
@Index(['search_id', 'link'], { unique: true })
export class SearchLinkEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'integer' })
  @Index()
  search_id!: number;

  @Column({ type: 'integer', nullable: true })
  @Index()
  channel_match_id?: number | null;

  @Column({ type: 'varchar', length: 512 })
  link!: string;

  @Column({ type: 'varchar', length: 32 })
  link_type!: string;

  @Column({ type: 'datetime', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
