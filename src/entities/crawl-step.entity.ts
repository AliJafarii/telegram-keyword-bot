import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'crawl_steps' })
@Index(['search_id', 'created_at'])
export class CrawlStepEntity {
  @PrimaryGeneratedColumn({ type: 'number' })
  id!: number;

  @Column({ type: 'number' })
  search_id!: number;

  @Column({ type: 'varchar2', length: 128 })
  step!: string;

  @Column({ type: 'clob', nullable: true })
  details?: string;

  @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
