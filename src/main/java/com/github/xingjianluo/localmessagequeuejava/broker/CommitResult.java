package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 位点提交结果。
 *
 * @param success   是否成功
 * @param reason    失败原因；成功为 null
 * @param committed 提交后该组在该分区的已确认位点（下一个待消费位点）
 */
public record CommitResult(boolean success, CommitRejectionReason reason, long committed) {

    public static CommitResult ok(long committed) {
        return new CommitResult(true, null, committed);
    }

    public static CommitResult reject(CommitRejectionReason reason, long committed) {
        return new CommitResult(false, reason, committed);
    }
}
