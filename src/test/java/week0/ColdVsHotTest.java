package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColdVsHotTest {

    // Cold: subscribe할 때마다 독립적으로 새로 실행된다
    @Test
    void coldFluxRunsPerSubscriber() {
        Flux<Integer> coldFlux = Flux.range(1, 3);

        // 첫 번째 구독
        StepVerifier.create(coldFlux)
                .expectNext(1, 2, 3)
                .expectComplete()
                .verify();

        // 두 번째 구독 — 1, 2, 3부터 다시 시작한다
        StepVerifier.create(coldFlux)
                .expectNext(1, 2, 3)
                .expectComplete()
                .verify();
    }

    // Hot: 이미 흐르고 있다. 늦게 subscribe한 구독자는 이전 데이터를 받지 못한다
    @Test
    void hotFluxDoesNotReplayMissedItems() {
        Sinks.Many<String> sink = Sinks.many().multicast().directAllOrNothing();
        Flux<String> hotFlux = sink.asFlux();

        // 구독 전에 emit
        sink.tryEmitNext("before-subscribe");

        List<String> received = new ArrayList<>();
        hotFlux.subscribe(received::add);

        // 구독 후 emit
        sink.tryEmitNext("after-subscribe");
        sink.tryEmitComplete();

        // 구독 전 데이터("before-subscribe")는 받지 못한다
        assertEquals(List.of("after-subscribe"), received,
                "Expected [after-subscribe], got: " + received);
    }

    // ❌ 의도적 실패 케이스 — 주석 해제 후 실행해서 StepVerifier 실패 메시지 확인
    // Cold Flux가 두 번째 구독 시 데이터를 emit하지 않는다는 잘못된 가정
//    @Test
//    void coldFluxWrongAssumption() {
//        Flux<Integer> coldFlux = Flux.range(1, 3);
//
//        StepVerifier.create(coldFlux)
//                .expectNext(1, 2, 3)
//                .expectComplete()
//                .verify();
//
//        // FAIL: Cold Flux는 두 번째 구독에서도 1, 2, 3을 새로 emit한다
//        StepVerifier.create(coldFlux)
//                .expectComplete()
//                .verify();
//    }
}
