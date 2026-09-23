package com.github.xingjianluo.localmessagequeuejava.mq;

/**
 * 位点提交结果。失败时 reason 给出可区分的拒绝原因。
 */
public record CommitResult(boolean accepted, long committedOffset, Reason reason) {

    public enum Reason {
        NONE,
        UNKNOWN_GROUP,
        OFFSET_OUT_OF_RANGE,
        OFFSET_REGRESSION
    }

    public static CommitResult ok(long committedOffset) {
        return new CommitResult(true, committedOffset, Reason.NONE);
    }

    public static CommitResult reject(Reason reason, long committedOffset) {
        return new CommitResult(false, committedOffset, reason);
    }
}
