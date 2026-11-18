# Schema v1
SQLAlchemy metadata creates users/audits/batches/mappings/runs/target_rows. Unique natural names, foreign keys, and target unique(run_id,key_digest) enforce linkage. Successful workers atomically commit accepted target rows; cancelled/failed runs do not persist partial targets. Restore replaces a run's target set under a run row lock and checks the logical backup checksum before commit.

# Schema v2 — 下游接库（全量分批 + 增量连续序）
新增 change_meta/change_events/subscriptions/delivery_batches/downstream_rows/applied_events，并为 target_rows 增加 born_seq。

- change_meta：单行计数器。迁移提交与备份恢复在同一事务内 `FOR UPDATE` 锁行顺序取号，变更序连续无缺口；提交顺序即下游交付顺序。
- change_events：每条目标行落库（kind=upsert）或备份恢复（kind=restore，即使逐行同值也记录）产生一条事件，含 seq/key_digest/payload/content_hash。
- subscriptions：下游接库单。snapshot_seq 为接库水位，全量只交 born_seq<=水位的目标行；snapshot_after_id 为 keyset 分页游标，每批独立短事务；stream_acked_seq 为增量已确认序；retained_from 为当前最早保留序；retention_limit 可配置；rebuild_reason/rebuild_history 留痕重建原因。
- delivery_batches：交付批次，唯一(subscription_id,batch_no)。下游回执丢失时重投同一开放批次（batch_no/checksum 不变），ack 后才推进增量游标，重复 ack 幂等。
- downstream_rows/applied_events：模拟下游库。自然键(subscription_id,key_digest)幂等 upsert，applied_events 以事件 seq 去重，重试不重复入库。
- 回收：按所有活跃接库单 retention_limit 的最小值全局裁剪 change_events，保证任何单子都不会被跳过缺口；拉取发现期望序低于 retained_from 或实际下一条不连续时，置 rebuild_required(gap_detected)，禁止跳过，只能重建。重建清空下游与账本并按最新水位重新全量。

PostgreSQL 依赖行级锁（with_for_update）与短事务；SQLite 仅用于本地等价验证（FOR UPDATE 被方言忽略，需开启 WAL 避免库级写锁互斥）。
