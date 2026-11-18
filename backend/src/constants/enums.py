ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed','cancelled')
TYPES={'sample_code':'text','test_name':'text','result_value':'decimal','unit':'text','measured_at':'datetime'}
# 下游接库单生命周期：全量分批 -> 增量连续 -> 掉队重建/人工重建
SUB_STATUSES=('snapshotting','streaming','rebuild_required','rebuilding','closed')
# 变更类别：迁移落库为upsert，备份恢复即使同值也单独记一条restore
CHANGE_KINDS=('upsert','restore')
# 交付批次阶段：snapshot=全量分批，stream=增量连续序
BATCH_PHASES=('snapshot','stream')
# 重建原因码，前端按码给中文解释
REBUILD_REASONS=('gap_detected','manual_rebuild','subscription_reset')
