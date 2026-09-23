package com.github.xingjianluo.localmessagequeuejava.web;

import com.github.xingjianluo.localmessagequeuejava.broker.CommitResult;
import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.broker.LocalMessageBroker;
import com.github.xingjianluo.localmessagequeuejava.broker.Message;
import com.github.xingjianluo.localmessagequeuejava.broker.PublishResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
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
 * 本地消息队列 HTTP 入口，用于手工验证投递 / 确认 / 重试 / 死信流程。
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final LocalMessageBroker broker;

    public QueueController(LocalMessageBroker broker) {
        this.broker = broker;
    }

    @PostMapping("/topics")
    public Map<String, Object> createTopic(@RequestBody TopicRequest req) {
        broker.createTopic(req.topic(), req.partitions());
        return Map.of("topic", req.topic(), "partitions", req.partitions());
    }

    @PostMapping("/subscriptions")
    public Map<String, Object> subscribe(@RequestBody SubscriptionRequest req) {
        broker.subscribe(req.group(), req.topic());
        return Map.of("group", req.group(), "topic", req.topic());
    }

    @PostMapping("/publish")
    public PublishResult publish(@RequestBody PublishRequest req) {
        return broker.publish(req.topic(), req.partition(), req.key(), req.dedupKey(), req.payload());
    }

    @GetMapping("/poll")
    public List<Message> poll(@RequestParam String group, @RequestParam String topic,
                              @RequestParam int partition, @RequestParam(defaultValue = "10") int max) {
        return broker.poll(group, topic, partition, max);
    }

    @PostMapping("/commit")
    public CommitResult commit(@RequestBody OffsetRequest req) {
        return broker.commit(req.group(), req.topic(), req.partition(), req.offset());
    }

    @PostMapping("/nack")
    public Message nack(@RequestBody OffsetRequest req) {
        return broker.nack(req.group(), req.topic(), req.partition(), req.offset());
    }

    @GetMapping("/offsets")
    public Map<String, Object> offsets(@RequestParam String group, @RequestParam String topic,
                                       @RequestParam int partition) {
        return Map.of(
                "committed", broker.committedOffset(group, topic, partition),
                "logEnd", broker.logEndOffset(topic, partition));
    }

    @GetMapping("/dead-letters")
    public List<DeadLetter> deadLetters(@RequestParam String topic) {
        return broker.deadLetters(topic);
    }

    @GetMapping("/dead-letters/{messageId}")
    public ResponseEntity<DeadLetter> deadLetter(@PathVariable String messageId) {
        return broker.deadLetter(messageId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgument(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    public record TopicRequest(String topic, int partitions) {
    }

    public record SubscriptionRequest(String group, String topic) {
    }

    public record PublishRequest(String topic, int partition, String key, String dedupKey, String payload) {
    }

    public record OffsetRequest(String group, String topic, int partition, long offset) {
    }
}
