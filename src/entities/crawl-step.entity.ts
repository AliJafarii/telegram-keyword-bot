import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'crawl_steps' })
@Index(['search_id', 'created_at'])
export class CrawlStepEntity {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'integer' })
  search_id!: number;

  @Column({ type: 'varchar', length: 128 })
  step!: string;

  @Column({ type: 'text', nullable: true })
  details?: string;

  @Column({ type: 'datetime', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
