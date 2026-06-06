package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

class StepVerifierTest {

    // 기본 흐름: expectNext + expectComplete + verify
    @Test
    void expectNextVerifiesValues() {
        StepVerifier.create(Flux.just("a", "b", "c"))
                .expectNext("a", "b", "c")
                .expectComplete()
                .verify();
    }

    // 에러 검증
    @Test
    void expectErrorVerifiesException() {
        Flux<String> flux = Flux.error(new RuntimeException("boom"));

        StepVerifier.create(flux)
                .expectError(RuntimeException.class)
                .verify();
    }

    // Backpressure: 초기 demand를 0으로 설정해서 수동으로 요청량을 제어
    @Test
    void backpressureWithThenRequest() {
        Flux<Integer> flux = Flux.range(1, 5);

        StepVerifier.create(flux, 0)  // 초기 demand = 0 (아무것도 요청하지 않음)
                .thenRequest(2)       // 2개 요청
                .expectNext(1, 2)
                .thenRequest(1)       // 1개 더 요청
                .expectNext(3)
                .thenCancel()         // 나머지(4, 5)는 취소
                .verify();
    }

    // 가상 시간: 실제 시간 대기 없이 시간 기반 연산자 테스트
    // withVirtualTime 없이 Mono.delay(10s)를 테스트하면 10초를 실제로 기다려야 한다
    @Test
    void withVirtualTimeForDelays() {
        StepVerifier.withVirtualTime(() ->
                        Mono.delay(Duration.ofSeconds(10)).map(t -> "delayed"))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(9))  // 9초 동안 이벤트 없음
                .thenAwait(Duration.ofSeconds(1))      // 가상 시간으로 1초 앞으로
                .expectNext("delayed")
                .expectComplete()
                .verify();
    }
}