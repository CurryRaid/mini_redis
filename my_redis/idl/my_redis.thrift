namespace rs volo.redis

enum RedisCommand {
    Get,
    Set,
    Ping,
    Del,
    Publish,
    Subscribe,
    Unkonwn,
}

struct GetItemRequest {
    1: required RedisCommand cmd,
    2: optional list<string> args,
}
struct GetItemResponse {
    1: required bool ok,
    2: optional string data,
    // 3: optional map<string, string> extra,
}
service ItemService {
    GetItemResponse GetItem (1: GetItemRequest req),
}