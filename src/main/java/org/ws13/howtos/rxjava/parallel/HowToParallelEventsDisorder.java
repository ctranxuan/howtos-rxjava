package org.ws13.howtos.rxjava.parallel;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.schedulers.Schedulers;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

/**
 * @author ctranxuan
 */
public class HowToParallelEventsDisorder {
    private static final Logger LOGGER = LogManager.getLogger(HowToParallelEventsDisorder.class);

    public static void main(String[] args) {
        parallelWithFlatMap();
//        parallelWithGroupBy();
//        parallelWithParallelAndOneSubscriber();
//        parallelWithParallelAndMultipleSubscribers();

        Flowable.timer(2, MINUTES)
                .blockingSubscribe();
    }

    private static void parallelWithParallelAndMultipleSubscribers() {
        List<String> list = new CopyOnWriteArrayList<>(Arrays.asList("a", "c", "d", "e", "f", "g"));
        Subscriber<String>[] subscribers =
                new MySubscriber[] {
                        new MySubscriber(), new MySubscriber(), new MySubscriber()
                };

        /*
         *  This demonstrates the parallelism with the use of parallel().
         *  Note the disorder of the event after b has been added is more perceptible than
         *  the sample with flatMap().
         */
        Flowable
                .fromIterable(list)
                .buffer(2)
                .delay(5, SECONDS)
                .repeat()
                .parallel(3)
                .runOn(Schedulers.io())
                //   .doOnNext(LOGGER::info)
                .map(HowToParallelEventsDisorder::slowOp)
                .doOnComplete(() -> LOGGER.info("------------> end"))
                .subscribe(subscribers);

        Flowable.timer(30, SECONDS)
                .doOnNext(l -> LOGGER.info("========= adding b"))
                .doOnNext(l -> list.add(1, "b"))
                .subscribe();
    }

    private static void parallelWithParallelAndOneSubscriber() {
        List<String> list = new CopyOnWriteArrayList<>(Arrays.asList("a", "c", "d", "e", "f", "g"));
        AtomicInteger counter = new AtomicInteger(0);

        /*
         *  This demonstrates the parallelism with the use of parallel().
         *  Note the disorder of the event after b has been added is more perceptible than
         *  the sample with flatMap().
         */
        Flowable.just(counter)
                .doOnNext(AtomicInteger::incrementAndGet)
                .flatMap(c -> Flowable.fromIterable(list)
                                      .map(s -> Tuple.of(c.get(), s)))
                .buffer(2)
                .delay(5, SECONDS)
                .repeat()
                .parallel(3)
                .runOn(Schedulers.io())
                //   .doOnNext(LOGGER::info)
                .map(HowToParallelEventsDisorder::slowOp2)
                .sequential()
                .doOnComplete(() -> LOGGER.info("------------> end"))
                .subscribe(LOGGER::info);

        Flowable.timer(30, SECONDS)
                .doOnNext(l -> LOGGER.info("========= adding b"))
                .doOnNext(l -> list.add(1, "b"))
                .subscribe();
    }

    private static void parallelWithFlatMap() {
        List<String> list = new CopyOnWriteArrayList<>(Arrays.asList("a", "c", "d", "e", "f", "g"));
        AtomicInteger counter = new AtomicInteger(0);

        /*
         * This demonstrates the parallelism with the use of the flatMap().
         * To show the potential disorder of the emission when b is added to
         * the list, the delay has been decreased to 1 sec.
         * This is not the case for the others (groupBy() and parallel()).
         */
        Flowable.just(counter)
                .doOnNext(AtomicInteger::incrementAndGet)
                .flatMap(c -> Flowable.fromIterable(list)
                                      .map(s -> Tuple.of(c.get(), s)))
                .buffer(2)
                .delay(1, SECONDS)
                .repeat()
                .flatMap(l -> Flowable.just(l)
                                      .subscribeOn(Schedulers.io())
//                                      .doOnNext(LOGGER::info)
                                      .map(HowToParallelEventsDisorder::slowOp2), 3)
                .doOnComplete(() -> LOGGER.info("------------> end"))
                .subscribe(LOGGER::info);

//        Flowable
//                .fromIterable(list)
//                .buffer(2)
//                .delay(5, SECONDS)
//                .repeat()
//                .flatMap(l -> Flowable.just(l)
//                                      .observeOn(Schedulers.io())
////                                      .doOnNext(LOGGER::info)
//                                      .map(HowToParallelEventsDisorder::slowOp)
//                                      .map(s -> format("%s: %s", counter.get(), s)), 3)
//                .doOnComplete(() -> LOGGER.info("------------> end"))
//                .subscribe(LOGGER::info);

        Flowable.timer(30, SECONDS)
                .doOnNext(l -> LOGGER.info("========= adding b"))
                .doOnNext(l -> list.add(1, "b"))
                .subscribe();
    }

    private static void parallelWithGroupBy() {
        List<String> list = new CopyOnWriteArrayList<>(Arrays.asList("a", "c", "d", "e", "f", "g"));
        AtomicInteger key = new AtomicInteger();

        AtomicInteger counter = new AtomicInteger(0);
        /*
         * see http://tomstechnicalblog.blogspot.fr/2015/11/rxjava-achieving-parallelization.html?showComment=1446751614332#c5136442252104578988
         * see http://tomstechnicalblog.blogspot.fr/2016/02/rxjava-maximizing-parallelization.html
         *
         *  This demonstrates the parallelism with the use of the groupBy() and flatMap().
         *  Note the disorder of the event after b has been added is more perceptible than
         *  the sample with flatMap().
         *
         *  Note the use of the observerOn() and not subscribeOn() due to the fact
         *  GroupedObservable specifies its own subscribeOn() thread and can’t be overridden
         *  (see one of the articles above).
         */
        Flowable.just(counter)
                .doOnNext(AtomicInteger::incrementAndGet)
                .flatMap(c -> Flowable.fromIterable(list)
                                      .map(s -> Tuple.of(c.get(), s)))
                .buffer(2)
                .delay(5, SECONDS)
                .repeat()
                .groupBy(v -> key.getAndIncrement() % 3)
                .flatMap(g -> g.observeOn(Schedulers.io())
//                               .doOnNext(LOGGER::info)
                               .map(HowToParallelEventsDisorder::slowOp2))
                .doOnComplete(() -> LOGGER.info("------------> end"))
                .subscribe(LOGGER::info);

//        Flowable
//                .fromIterable(list)
//                .buffer(2)
//                .delay(5, SECONDS)
//                .repeat()
//                .groupBy(v -> key.getAndIncrement() % 3)
//                .flatMap(g -> g.observeOn(Schedulers.io())
//                               .doOnNext(LOGGER::info)
//                               .map(HowToParallelEventsDisorder::slowOp))
//                .doOnComplete(() -> LOGGER.info("------------> end"))
//                .subscribe(LOGGER::info);

        Flowable.timer(30, SECONDS)
                .doOnNext(l -> LOGGER.info("========= adding b"))
                .doOnNext(l -> list.add(1, "b"))
                .subscribe();
    }

    private static String slowOp(final List<String> aStrings) throws InterruptedException {
        if (aStrings.contains("d")) {
            Thread.sleep(10000);
        }

        return aStrings.stream()
                       .collect(joining(","));
    }

    private static String slowOp2(final List<Tuple2<Integer, String>> aStrings) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        List<String> strings = aStrings.stream()
                                       .map(t -> {
                                            counter.set(t._1);
                                            return t._2;
                                       })
                                       .collect(Collectors.toList());

        if (strings.contains("d")) {
            Thread.sleep(10000);
        }

        return counter
                + ": "
                + strings.stream()
                        .collect(joining(","));
    }

    static class MySubscriber implements FlowableSubscriber<String> {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void onSubscribe(final Subscription aSubscription) {
            LOGGER.info("MySubscriber.onSubscribe");
            aSubscription.request(MAX_VALUE); // that's the trick, 1 is not enough...
        }

        @Override
        public void onNext(final String aS) {
            LOGGER.info("{}: {}", counter.incrementAndGet(), aS);
        }

        @Override
        public void onError(final Throwable aThrowable) {
            LOGGER.error(aThrowable);
        }

        @Override
        public void onComplete() {
            LOGGER.info("onComplete");
        }
    }
}
