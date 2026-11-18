export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待处理',running:'迁移中',completed:'已完成',failed:'失败',cancelled:'已取消'};
export const TYPES={sample_code:'text',test_name:'text',result_value:'decimal',unit:'text',measured_at:'datetime'};
export const SUB_STATUSES={snapshotting:'全量抄录中',streaming:'增量同步中',rebuild_required:'需重建',rebuilding:'重建全量中',closed:'已关闭'};
export const CHANGE_KINDS={upsert:'迁移落库',restore:'备份恢复'};
export const REBUILD_REASONS={gap_detected:'增量缺口被回收，无法续传，按水位重建全量',manual_rebuild:'人工发起重建，下游库清空后重新接库',subscription_reset:'接库单重置，重新抄录全量'};
