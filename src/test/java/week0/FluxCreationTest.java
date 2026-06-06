package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

class FluxCreationTest {

    // just: 미리 알고 있는 값들을 바로 emit
    @Test
    void justEmitsStaticValues() {
        Flux<String> flux = Flux.just("a", "b", "c");

        StepVerifier.create(flux)
                .expectNext("a", "b", "c")
                .expectComplete()
                .verify();
    }

    // fromIterable: 컬렉션을 Flux로 변환
    @Test
    void fromIterableEmitsCollection() {
        Flux<Integer> flux = Flux.fromIterable(List.of(10, 20, 30));

        StepVerifier.create(flux)
                .expectNext(10, 20, 30)
                .expectComplete()
                .verify();
    }

    // create: FluxSink를 통해 프로그래밍 방식으로 emit. 외부 콜백/리스너 연동에 적합
    // just와 달리 sink를 외부로 전달해 비동기 콜백 안에서 next/error/complete를 호출할 수 있다
    @Test
    void createEmitsProgrammatically() {
        Flux<String> flux = Flux.create(sink -> {
            sink.next("x");
            sink.next("y");
            sink.complete();
        });

        StepVerifier.create(flux)
                .expectNext("x", "y")
                .expectComplete()
                .verify();
    }

    // generate: 상태를 유지하며 한 번에 하나씩 emit. 무한 스트림 생성에 적합
    @Test
    void generateEmitsSequentially() {
        Flux<Integer> flux = Flux.<Integer, Integer>generate(
                () -> 0,                      // 초기 상태
                (state, sink) -> {
                    sink.next(state * state);  // state^2 emit
                    return state + 1;          // 다음 상태 반환
                }
        ).take(5);                            // 무한 스트림을 5개로 제한

        StepVerifier.create(flux)
                .expectNext(0, 1, 4, 9, 16)
                .expectComplete()
                .verify();
    }
}