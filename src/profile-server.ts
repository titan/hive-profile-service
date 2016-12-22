import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response, msgpack_decode, msgpack_encode, rpc } from "hive-service";
import * as Redis from "redis";
import { Client as PGClient } from "pg";
import * as http from "http";
import * as bunyan from "bunyan";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, numberVerifier } from "hive-verify";
import * as uuid from "node-uuid";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

let log = bunyan.createLogger({
  name: "profile-server",
  streams: [
    {
      level: "info",
      path: "/var/log/profile-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/profile-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let list_key = "profile";
let entity_key = "profile-entities";
let wxuser_key = "wxuser";

export const server = new Server();

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

const tickets = [
  "gQH57zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1REbklHUDdtOGJHSV9pS3k4QldyAAIEMGApWAMEAAAAAA==",
  "gQHa8DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xLzN6bDFNQ1BtVExFMW5MR2FUUldyAAIE1ugfWAMEAAAAAA==",
  "gQFw7zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL0pqbFZKZ1BtUzdFeVUwaXJiUldyAAIELnEdWAMEAAAAAA==",
  "gQFw7zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL0pqbFZKZ1BtUzdFeVUwaXJiUldyAAIELnEdWAMEAAAAAA==",
  "gQE78DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1BUbGtSc1RtUkxFOTcxUDFYQldyAAIEMFEQWAMEAAAAAA==",
  "gQFA8DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1l6a0RDb1htRHJGM3ZBMlVPeFdyAAIEJ9z9VwMEAAAAAA=="
];


server.call("getUser", allowAll, "获得当前用户信息", "获得当前用户信息", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("getUser " + ctx.uid);
  ctx.cache.hget(entity_key, ctx.uid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      (async () => {
        try {
          let user = await msgpack_decode(result);
          rep({ code: 200, data: user });
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
    } else {
      rep({ code: 404, msg: "not found user" });
    }
  });
});

server.call("getDiscountStatus", allowAll, "获得当前用户信息", "获得当前用户优惠情况", (ctx: ServerContext, rep: ((result: any) => void), recommend: string) => {
  log.info("getDiscountStatus " + ctx.uid);
  ctx.cache.hget(entity_key, ctx.uid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      (async () => {
        try {
          const user = await msgpack_decode(result);
          if (user["ticket"] && user["ticket"] !== "") {
            for (let ticket of tickets) {
              if (user["ticket"] === ticket) {
                rep({ code: 200, data: true });
                return;
              } else if (recommend && recommend !== "") {
                if (recommend === ticket) {
                  rep({ code: 200, data: true });
                  return;
                }
              }
            }
            rep({ code: 200, data: false });
          } else if (recommend && recommend !== "") {
            for (let ticket of tickets) {
              if (recommend === ticket) {
                rep({ code: 200, data: true });
                return;
              }
            }
          } else {
            rep({ code: 200, data: false });
          }
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
    } else {
      rep({ code: 404, msg: "not found user" });
    }
  });
});

server.call("getUserForInvite", allowAll, "获取邀请好友信息", "获取邀请好友信息", (ctx: ServerContext, rep: ((result: any) => void), key: string) => {
  log.info("getUserForInvite " + key);
  ctx.cache.get("InviteKey:" + key, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      ctx.cache.hget(entity_key, result, function (err2, result2) {
        if (err2) {
          rep({ code: 500, msg: err2.message });
        } else if (result2) {
          (async () => {
            try {
              let user = await msgpack_decode(result2);
              rep({ code: 200, data: user });
            } catch (e) {
              log.error(e);
              rep({ code: 500, msg: e.message });
            }
          })();
        } else {
          rep({ code: 404, msg: "not found user" });
        }
      });
    } else {
      rep({ code: 404, msg: "not found invitekey" });
    }
  });
});

server.call("getUserByUserId", allowAll, "根据userid获得某个用户信息", "根据userid获得某个用户信息", (ctx: ServerContext, rep: ((result: any) => void), user_id: string) => {
  log.info("getUserByUserId " + ctx.uid);
  if (!verify([uuidVerifier("user_id", user_id)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(entity_key, user_id, function (err, result) {
    if (err || !result) {
      rep({ code: 500, msg: err.message });
    } else {
      (async () => {
        try {
          let user = await msgpack_decode(result);
          rep({ code: 200, data: user });
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
    }
  });
});

server.call("getUserOpenId", allowAll, "获取某个用户的openid", "获取某个用户的openid", (ctx: ServerContext, rep: ((result: any) => void), uid: string) => {
  log.info("getUserOpenId, ctx.uid:" + ctx.uid + " arg uid:" + uid);
  if (!verify([uuidVerifier("uid", uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(wxuser_key, uid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      rep({ code: 200, data: result });
    }
  });
});

server.call("refresh", allowAll, "refresh", "refresh", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("refresh " + ctx.uid);
  const callback = uuid.v1();
  const pkt: CmdPacket = { cmd: "refresh", args: [callback] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("getAllUsers", allowAll, "获取所有用户信息", "获取所有用户信息", (ctx: ServerContext, rep: ((result: any) => void), start: number, limit: number) => {
  log.info("getAllUsers" + "uid is " + ctx.uid);
  if (!verify([numberVerifier("start", start), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.lrange(list_key, start, limit, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      let multi = ctx.cache.multi();
      for (let id of result) {
        multi.hget(entity_key, id);
      }
      multi.exec(function (err, replies) {
        if (err) {
          log.info("multi err: " + err);
          rep({ code: 500, msg: err.message });
        } else {
          (async () => {
            try {
              let users = [];
              for (let reply of replies) {
                let user = await msgpack_decode(reply);
                users.push(user);
              }
              rep({ code: 200, data: users });
            } catch (e) {
              log.error(e);
              rep({ code: 500, msg: e.message });
            }
          })();
          rep({ code: 200, data: replies });
        }
      });
    }
  });
});

server.call("getUserByUserIds", allowAll, "根据userid数组获得一些用户信息", "根据userid数组获得一些用户信息", (ctx: ServerContext, rep: ((result: any) => void), user_ids) => {
  log.info("getUserByUserIds " + ctx.uid);
  let multi = ctx.cache.multi();
  for (let user_id of user_ids) {
    multi.hget(entity_key, user_id);
  }
  multi.exec((err, result) => {
    if (err) {
      log.info(err);
      rep({ code: 500, msg: err.message });
    } else if (result) {
      let users = [];
      (async () => {
        try {
          let users = [];
          for (let u of result) {
            let user = await msgpack_decode(u);
            users.push(user);
          }
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
      let replies = {};
      for (let user of users) {
        replies[user.id] = user;
      }
      rep({ code: 200, data: replies });
    } else {
      log.info("not found users");
      rep({ code: 404, msg: "not found users" });
    }
  });
});

