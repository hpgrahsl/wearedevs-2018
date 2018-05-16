package com.github.hpgrahsl.kafka.controller;

import com.github.hpgrahsl.kafka.model.EmojiCount;
import com.github.hpgrahsl.kafka.service.StateStoreService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Set;

@RestController
@RequestMapping("interactive/queries/")
@CrossOrigin(origins = "*")
public class StateStoreController {

    private final StateStoreService service;

    public StateStoreController(StateStoreService service) {
        this.service = service;
    }

    @GetMapping("emojis/{code}")
    public Mono<ResponseEntity<EmojiCount>> getEmoji(@PathVariable String code) {
        return service.querySingleEmojiCount(code);
    }

    @GetMapping("local/emojis")
    public Flux<EmojiCount> getEmojisLocal() {
        return service.queryLocalEmojiCounts();
    }

    @GetMapping("emojis")
    public Flux<EmojiCount> getEmojis() {
        return service.queryAllEmojiCounts();
    }

    @GetMapping("emojis/stats/topN")
    public Mono<Set<EmojiCount>> getEmojisTopN() {
        return service.queryEmojiCountsTopN();
    }

    @GetMapping(path = "emojis/updates/notify", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<EmojiCount> getEmojiCountsStream() {
        return service.consumeEmojiCountsStream();
    }

    @GetMapping(path = "emojis/{code}/tweets", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Tuple2<String,String>> getEmojiTweetsStream(@PathVariable String code) {
        return service.consumeEmojiTweetsStream(code);
    }

}
