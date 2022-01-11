package org.spsc;

import com.google.gson.Gson;
import org.spsc.job.*;
import spark.Filter;

import static spark.Spark.after;
import static spark.Spark.get;

public class JobController {

    public static void main(String[] args) {

        after((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET");
        });

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

        get("/allVisitorsByDayWithoutGeo", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDayWithoutGeo.apiCall())));
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

        get("/valByID/:id", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(valByID.apiCall(request.params(":id")))));
        });

    }
}
