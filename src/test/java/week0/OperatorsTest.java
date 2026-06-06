package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers; // flatMapAsyncOrderWrongAssumption 주석 해제 시 사용
import reactor.test.StepVerifier;

class OperatorsTest {

    // map: 각 원소를 동기적으로 1:1 변환
    @Test
    void mapTransformsValues() {
        Flux<String> flux = Flux.just("hello", "world")
                .map(String::toUpperCase);

        StepVerifier.create(flux)
                .expectNext("HELLO", "WORLD")
                .expectComplete()
                .verify();
    }

    // flatMap: 각 원소를 Publisher로 변환하고 결과를 병합한다 (1:N 가능)
    // 동기 내부 Publisher에서는 순서가 유지된다
    @Test
    void flatMapExpandsEachElement() {
        Flux<String> flux = Flux.just("a", "b")
                .flatMap(s -> Flux.just(s, s.toUpperCase()));

        StepVerifier.create(flux)
                .expectNext("a", "A", "b", "B")
                .expectComplete()
                .verify();
    }

    // filter: 조건을 만족하는 원소만 통과
    @Test
    void filterKeepsMatchingValues() {
        Flux<Integer> flux = Flux.range(1, 6)
                .filter(n -> n % 2 == 0);

        StepVerifier.create(flux)
                .expectNext(2, 4, 6)
                .expectComplete()
                .verify();
    }

    // take: 앞에서 N개만 받고 onComplete 신호를 보낸다
    @Test
    void takeLimitsElements() {
        Flux<Integer> flux = Flux.range(1, 100)
                .take(3);

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectComplete()
                .verify();
    }

    // ❌ 의도적 실패 케이스 — 주석 해제 후 실행
    // 비동기 flatMap에서 순서가 보장된다는 잘못된 가정
//    @Test
//    void flatMapAsyncOrderWrongAssumption() {
//        Flux<String> flux = Flux.just("a", "b")
//                .flatMap(s -> reactor.core.publisher.Mono.just(s.toUpperCase())
//                        .subscribeOn(Schedulers.parallel()));
//
//        // FAIL: 비동기 flatMap은 먼저 완료되는 것이 먼저 emit된다 — 순서 불보장
//        StepVerifier.create(flux)
//                .expectNext("A", "B")
//                .expectComplete()
//                .verify();
//    }
}