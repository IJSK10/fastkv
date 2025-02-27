#include "server.h"

void start_server(HashMap& hashmap)
{
    crow::SimpleApp app;

    CROW_ROUTE(app,"/set").methods(crow::HTTPMethod::Post)([&](const crow::request& req){
        auto body=crow::json::load(req.body);
        if (!body) return crow::response(400,"Invalid JSON");

        std::string key = body["key"].s();
        std::string value = body["value"].s();
        int ttl = body.has("ttl") ? body["ttl"].i() : 0;

        hashmap.set(key,value,ttl);
        return crow::response(200,"Key set successfully");
    });

    CROW_ROUTE(app,"/get").methods(crow::HTTPMethod::Get)([&](const crow::request& req)
{
    auto key = req.url_params.get("key");
    if (!key) return crow::response(400,"Missing key");

    std::string value = hashmap.get(key);
    if (value.empty()) return crow::response(404,"Key not found"); 

    crow::json::wvalue res;
    res["key"] = key;
    res["value"] = value;
    return crow::response(res);
});

CROW_ROUTE(app,"/remove").methods(crow::HTTPMethod::Delete)([&](const crow::request& req){
    auto key = req.url_params.get("key");
    if (!key) return crow::response(400,"Missing key");

    bool success = hashmap.remove(key);
    return success ? crow::response(200,"Key removed") : crow::response(404,"Key not found");
});

app.port(8080).bindaddr("127.0.0.1").multithreaded().run();
}
