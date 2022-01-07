package org.spsc;

import com.google.gson.Gson;
import org.spsc.job.allVisitorsByDay;

import static spark.Spark.get;

public class HelloWorldService {

    //FIXME cambiare tutti i return type delle query se sono long restituiti da una count non c'è problema,
    // se sono i risultati di una groupby devo fare la formattazione json

    public static void main(String[] args) {

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

        get("/hello", (request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, new Gson().toJsonTree(allVisitorsByDay.apiCall())));
        });

    }
}
