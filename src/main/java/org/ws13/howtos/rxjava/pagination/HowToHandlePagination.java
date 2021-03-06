package org.ws13.howtos.rxjava.pagination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author ctranxuan
 */
public class HowToHandlePagination {
    static class FakeHttpClient {
        private final Map<String, JsonNode> responses;
        private final static ObjectMapper MAPPER = new ObjectMapper();

        public FakeHttpClient() {
            responses = new HashMap<>();


            JsonNode response1 = MAPPER.createObjectNode()
                                       .put("next", "http://url2")
                                       .set("data", MAPPER.createArrayNode()
                                                          .add("Doc")
                                                          .add("Grumpy")
                                                          .add("Happy"));
            responses.put("http://url1", response1);

            JsonNode response2 = MAPPER.createObjectNode()
                                       .put("next", "http://url3")
                                       .set("data", MAPPER.createArrayNode()
                                                          .add("Sleepy")
                                                          .add("Dopey")
                                                          .add("Bashful"));
            responses.put("http://url2", response2);

            JsonNode response3 = MAPPER.createObjectNode()
                                       .set("data", MAPPER.createArrayNode()
                                                          .add("Sneezy"));
            responses.put("http://url3", response3);
        }

        public JsonNode callApi(final String aUrl) {
            return responses.get(aUrl);
        }
    }

    static class User {
        private final String userName;

        User(final String aUserName) {
            userName = aUserName;
        }

        public String getUserName() {
            return userName;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userName='" + userName + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {
        FakeHttpClient httpClient = new FakeHttpClient();

        /*
         * Basic sample to deal with an API that exposes data with pagination
         * ... and you want to get the whole set of data.
         *
         * Some null checking have not been implemented (use Optional or whatever)
         * to keep the sample simple: the aim was to demonstrate a way of doing that.
         */
        AtomicReference<JsonNode> ref = new AtomicReference<>();

        Flowable.just(ref)
                .map(AtomicReference::get)
                .map(json -> json.get("next")
                                 .asText())
                .doOnNext(url -> System.out.println("calling url: " + url))
                .map(httpClient::callApi)
                .doOnNext(data -> System.out.println("result of api call: " + data))
                .doOnNext(ref::set)
                .repeatUntil(() -> ref.get() != null && ref.get()
                                                           .get("next") == null)
                .startWith(Flowable.just("http://url1")
                                   .doOnNext(url -> System.out.println("calling url: " + url))
                                   .map(httpClient::callApi)
                                   .doOnNext(data -> System.out.println("result of api call: " + data))
                                   .doOnNext(ref::set))
                .map(json -> json.get("data"))
                .doOnNext(System.out::println)
                .concatMap(Flowable::fromIterable)
                .map(el -> new User(el.asText()))
                .toList()
                .subscribe(System.out::println);

//        prettierWayToHandlePagination();

        Flowable.timer(30, SECONDS)
                .blockingSubscribe();

    }

    private static void prettierWayToHandlePagination() {
        /*
         * Below, a refactored version.
         * This is just a rewritting that extracts the common part between the repeat() and the startsWith().
         * A bit more readable in my opinion.
         */
        FakeHttpClient httpClient = new FakeHttpClient();
        AtomicReference<JsonNode> ref = new AtomicReference<>();
        FlowableTransformer<String, JsonNode> callApi = callApi(httpClient, ref);

        Flowable.just(ref)
                .map(AtomicReference::get)
                .map(json -> json.get("next").asText())
                .compose(callApi)
                .repeatUntil(() -> ref.get() != null && ref.get()
                                                           .get("next") == null)
                .startWith(Flowable.just("http://url1").compose(callApi))
                .map(json -> json.get("data"))
                .doOnNext(System.out::println)
                .concatMap(Flowable::fromIterable)
                .map(el -> new User(el.asText()))
                .toList()
                .subscribe(System.out::println);
    }

    private static FlowableTransformer<String, JsonNode> callApi(FakeHttpClient aHttpClient, AtomicReference<JsonNode> aRef) {
        return (upstream) -> upstream
                .doOnNext(url -> System.out.println("calling url: " + url))
                .map(aHttpClient::callApi)
                .doOnNext(data -> System.out.println("result of api call: " + data))
                .doOnNext(aRef::set);
    }

}