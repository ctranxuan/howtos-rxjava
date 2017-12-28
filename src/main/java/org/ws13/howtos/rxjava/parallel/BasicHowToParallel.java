package org.ws13.howtos.rxjava.parallel;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author ctranxuan
 */
public class BasicHowToParallel {
    private static final Logger LOGGER = LogManager.getLogger(HowToParallelEventsDisorder.class);

    public static void main(String[] args) {
        Flowable<Integer> source = Flowable.range(1, 6);

        source
                .parallel(2)
                .runOn(Schedulers.io())
                .doOnNext(LOGGER::info)
                .map(BasicHowToParallel::slowOp)
                .sequential()
                .subscribe(n -> LOGGER.info("result: {}", n));

        Flowable.timer(1, MINUTES)
                .blockingSubscribe();
    }

    private static String slowOp(final int aInt) throws InterruptedException {
        if (aInt % 2 == 0) {
            Thread.sleep(10000);
        }

        return "Hello " + aInt;
    }
}
