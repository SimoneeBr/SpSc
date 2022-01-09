package org.spsc;

import com.google.gson.Gson;
import org.spsc.job.*;

import static spark.Spark.get;

public class HelloWorldService {

    //FIXME cambiare tutti i return type delle query se sono long restituiti da una count non c'è problema,
    // se sono i risultati di una groupby devo fare la formattazione json

    //TODO potremmo avviare spark all'run di questo main con in singelton e poi tutti richiamano il metodo

    public static void main(String[] args) {

        get("/tweetsNLP", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(tweetsNLP.apiCall())));
        });

        get("/allVisitors", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitors.apiCall())));
        });

        get("/allVisitorsByDay", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/countryOfTweets", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(countryOfTweets.apiCall())));
        });
        get("/countryOfVisitors", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(countryOfVisitors.apiCall())));
        });
        get("/deviceOfTweets", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(deviceOfTweets.apiCall())));
        });

        get("/italianTweets", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(italianTweets.apiCall())));
        });
        get("/like_count", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(metricsTweets.api_like_count())));
        });
        get("/retweet_count", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(metricsTweets.api_retweet_count())));
        });
        get("/quote_count", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(metricsTweets.api_quote_count())));
        });
        get("/reply_count", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(metricsTweets.api_reply_count())));
        });
        get("/mostFrequentHashtag", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(mostFrequentHashtag.apiCall())));
        });
        get("/allTweetsByVerifiedVisitor", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(verifiedVisitors.allTweetsByVerifiedVisitorAPI())));
        });
        get("/allVerifiedVisitors", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(verifiedVisitors.allVerifiedVisitorsAPI())));
        });

    }
}
