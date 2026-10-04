package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 死信重放结果。
 *
 * @param success 是否成功重放
 * @param reason  失败原因；成功为 null
 * @param message 重放后重新入队的消息（携带新位点）；失败为 null
 */
public record ReplayResult(boolean success, ReplayRejectionReason reason, Message message) {

    public static ReplayResult ok(Message message) {
        return new ReplayResult(true, null, message);
    }

    public static ReplayResult reject(ReplayRejectionReason reason) {
        return new ReplayResult(false, reason, null);
    }
}
