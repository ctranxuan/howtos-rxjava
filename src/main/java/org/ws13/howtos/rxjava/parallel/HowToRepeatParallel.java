package org.ws13.howtos.rxjava.parallel;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

/**
 * @author ctranxuan
 */
public class HowToRepeatParallel {
    private static final Logger LOGGER = LogManager.getLogger(HowToParallelEventsDisorder.class);

    public static void main(String[] args) {
        /*
         * This illustrates that the parallel() performs operations in parallel :)
         * (thanks, Captain Obvious!),
         * but one sequence is complete only when all the operations have been completed
         * (especially, the sequence has to wait for the slowest operation).
         */
        List<String> list = new CopyOnWriteArrayList<>(Arrays.asList("a", "c", "d", "e", "f", "g"));

        Flowable
                .fromIterable(list)
                .buffer(2)
                .parallel(3)
                .runOn(Schedulers.io())
                //   .doOnNext(LOGGER::info)
                .map(HowToRepeatParallel::slowOp)
                .sequential()
                .doOnComplete(() -> LOGGER.info("------------> end"))
                .repeat()
                .subscribe(n -> LOGGER.info("result: {}", n));


        Flowable.timer(30, SECONDS)
                .doOnNext(l -> LOGGER.info("========= adding b"))
                .doOnNext(l -> list.add(1, "b"))
                .subscribe();

        Flowable.timer(2, MINUTES)
                .blockingSubscribe();
    }

    private static String slowOp(final List<String> aStrings) throws InterruptedException {
        if (aStrings.contains("d")) {
            Thread.sleep(10000);
        }

        return aStrings.stream()
                       .collect(joining(","));
    }
}
