package week0;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionLifecycleTest {

    // subscribe() 호출 전까지 Flux는 실행되지 않는다
    @Test
    void subscribeTriggersExecution() {
        AtomicBoolean executed = new AtomicBoolean(false);

        Flux<String> flux = Flux.defer(() -> {
            executed.set(true);
            return Flux.just("value");
        });

        assertFalse(executed.get(), "subscribe 전에는 실행되지 않는다");

        StepVerifier.create(flux)
                .expectNext("value")
                .expectComplete()
                .verify();

        assertTrue(executed.get(), "subscribe 후에는 실행된다");
    }

    // onNext → onComplete 순서가 보장된다
    @Test
    void lifecycleOrderIsGuaranteed() {
        List<String> events = new ArrayList<>();

        Flux.just("a", "b", "c")
                .subscribe(
                        item  -> events.add("next:" + item),
                        error -> events.add("error"),
                        ()    -> events.add("complete")
                );

        assertEquals(List.of("next:a", "next:b", "next:c", "complete"), events);
    }

    // 에러 발생 시 onError로 종료되고 이후 onNext는 호출되지 않는다
    @Test
    void onErrorTerminatesStream() {
        Flux<Integer> flux = Flux.just(1, 2, 3)
                .map(n -> {
                    if (n == 2) throw new RuntimeException("error at 2");
                    return n;
                });

        StepVerifier.create(flux)
                .expectNext(1)
                .expectError(RuntimeException.class)
                .verify();
    }

    // dispose() 호출 시 스트림이 중단된다
    @Test
    void disposeStopsStream() throws InterruptedException {
        List<Long> received = new ArrayList<>();

        Disposable disposable = Flux.interval(Duration.ofMillis(10))
                .subscribe(received::add);

        Thread.sleep(35);
        disposable.dispose();
        int sizeAfterDispose = received.size();
        Thread.sleep(35);

        assertTrue(sizeAfterDispose > 0, "dispose 전에 적어도 하나 이상 수신되어야 한다");
        assertEquals(sizeAfterDispose, received.size(), "dispose 후 추가 값이 수신되지 않는다");
        assertTrue(disposable.isDisposed());
    }
}