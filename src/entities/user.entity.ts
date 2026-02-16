import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'users' })
export class UserEntity {
  @PrimaryGeneratedColumn({ type: 'number' })
  id!: number;

  @Column({ type: 'varchar2', length: 64, unique: true })
  @Index()
  telegram_id!: string;

  @Column({ type: 'varchar2', length: 128, nullable: true })
  username?: string;

  @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
}
