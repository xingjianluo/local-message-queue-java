package com.github.xingjianluo.localmessagequeuejava.mq.web;

import com.github.xingjianluo.localmessagequeuejava.mq.CommitResult;
import com.github.xingjianluo.localmessagequeuejava.mq.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.mq.LocalMessageQueue;
import com.github.xingjianluo.localmessagequeuejava.mq.Message;
import com.github.xingjianluo.localmessagequeuejava.mq.PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地消息队列的 HTTP 演示接口：
 *  POST /mq/topics/{topic}?partitions=n               建主题
 *  POST /mq/topics/{topic}/groups/{group}             注册消费者组
 *  POST /mq/topics/{topic}/partitions/{partition}/publish   幂等发布
 *  GET  /mq/groups/{group}/topics/{topic}/partitions/{partition}/poll?max=n
 *  POST /mq/groups/{group}/topics/{topic}/partitions/{partition}/commit?offset=n
 *  POST /mq/groups/{group}/topics/{topic}/partitions/{partition}/nack?offset=n&error=...
 *  GET  /mq/groups/{group}/committed?topic=&partition=
 *  GET  /mq/groups/{group}/dead-letters
 */
@RestController
@RequestMapping("/mq")
public class QueueController {

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    private final LocalMessageQueue queue;

    public QueueController(LocalMessageQueue queue) {
        this.queue = queue;
    }

    public record PublishRequest(String key, String dedupKey, String payload) {
    }

    @PostMapping("/topics/{topic}")
    public Map<String, Object> createTopic(@PathVariable String topic,
                                           @RequestParam(defaultValue = "1") int partitions) {
        queue.createTopic(topic, partitions);
        return Map.of("topic", topic, "partitions", partitions);
    }

    @PostMapping("/topics/{topic}/groups/{group}")
    public Map<String, Object> registerGroup(@PathVariable String topic, @PathVariable String group) {
        queue.registerGroup(group, topic);
        return Map.of("group", group, "topic", topic);
    }

    @PostMapping("/topics/{topic}/partitions/{partition}/publish")
    public PublishResult publish(@PathVariable String topic,
                                 @PathVariable int partition,
                                 @RequestBody PublishRequest request) {
        PublishResult result = queue.publish(topic, partition, request.key(),
                request.dedupKey(), request.payload());
        log.info("[HTTP发布] key={} dedupKey={} offset={} 判定={}",
                request.key(), request.dedupKey(), result.message().offset(), result.status());
        return result;
    }

    @GetMapping("/groups/{group}/topics/{topic}/partitions/{partition}/poll")
    public List<Message> poll(@PathVariable String group, @PathVariable String topic,
                              @PathVariable int partition,
                              @RequestParam(defaultValue = "10") int max) {
        return queue.poll(group, topic, partition, max);
    }

    @PostMapping("/groups/{group}/topics/{topic}/partitions/{partition}/commit")
    public CommitResult commit(@PathVariable String group, @PathVariable String topic,
                               @PathVariable int partition, @RequestParam long offset) {
        CommitResult result = queue.commit(group, topic, partition, offset);
        log.info("[HTTP提交] group={} offset={} accepted={} reason={} committed={}",
                group, offset, result.accepted(), result.reason(), result.committedOffset());
        return result;
    }

    @PostMapping("/groups/{group}/topics/{topic}/partitions/{partition}/nack")
    public Map<String, Object> nack(@PathVariable String group, @PathVariable String topic,
                                    @PathVariable int partition, @RequestParam long offset,
                                    @RequestParam(defaultValue = "processing failed") String error) {
        queue.nack(group, topic, partition, offset, error);
        return Map.of("offset", offset, "nacked", true);
    }

    @GetMapping("/groups/{group}/committed")
    public Map<String, Object> committed(@PathVariable String group, @RequestParam String topic,
                                         @RequestParam int partition) {
        return Map.of("group", group, "topic", topic, "partition", partition,
                "committed", queue.committedOffset(group, topic, partition));
    }

    @GetMapping("/groups/{group}/dead-letters")
    public List<DeadLetter> deadLetters(@PathVariable String group) {
        return queue.deadLetters(group);
    }
}
