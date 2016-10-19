import { Server, Config, Context, ResponseFunction, Permission, wait_for_response } from "hive-server";
import { servermap } from "hive-hostmap";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as http from "http";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, numberVerifier } from "hive-verify";

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

let redis = Redis.createClient(6379, "redis"); // port, host

let list_key = "profile";
let entity_key = "profile-entities";
let wxuser_key = "wxuser";

let config: Config = {
  svraddr: servermap["profile"],
  msgaddr: "ipc:///tmp/profile.ipc"
};

let svc = new Server(config);

let permissions: Permission[] = [["mobile", true], ["admin", true]];

// 根据userid获得某个用户信息
svc.call("getUserInfoByUserId", permissions, (ctx: Context, rep: ResponseFunction, user_id: string) => {
  log.info("getUserInfoByUserId " + ctx.uid);
  if (!verify([uuidVerifier("user_id", user_id)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  redis.hget(entity_key, user_id, function (err, result) {
    if (err || !result) {
      rep({ code: 500, msg: err.message });
    } else {
      rep({ code: 200, data: JSON.parse(result) });
    }
  });
});

// 获得当前用户信息
svc.call("getUserInfo", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getUserInfo " + ctx.uid);
  redis.hget(entity_key, ctx.uid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      rep({ code: 200, data: JSON.parse(result) });
    }
  });
});

// 获取某个用户的openid
svc.call("getUserOpenId", permissions, (ctx: Context, rep: ResponseFunction, uid: string) => {
  log.info("getUserInfo, ctx.uid:" + ctx.uid + " arg uid:" + uid);
  if (!verify([uuidVerifier("uid", uid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  redis.hget(wxuser_key, uid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      rep({ code: 200, data: result });
    }
  });
});

// 添加用户信息
svc.call("addUserInfo", permissions, (ctx: Context, rep: ResponseFunction, uid: string, openid: string, gender: string, nickname: string, portrait: string) => {
  log.info("setUserInfo " + ctx.uid);
  if (!verify([uuidVerifier("uuid", uid), stringVerifier("openid", openid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let args = [uid, openid, gender, nickname, portrait];
  ctx.msgqueue.send(msgpack.encode({ cmd: "addUserInfo", args: args }));
  log.info("addUserInfo" + args);
  rep({ code: 200, msg: "sucessful" });
});


// refresh
svc.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("refresh " + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: null }));
  rep({ code: 200, data: "sucessful" });
});

// 获取所有用户信息
svc.call("getAllUsers", permissions, (ctx: Context, rep: ResponseFunction, start: number, limit: number) => {
  log.info("getAllUsers" + "uid is " + ctx.uid);
  if (!verify([numberVerifier("start", start), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  redis.lrange(list_key, start, limit, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      log.info("getAllUsers result" + result);
      ids2objects(entity_key, result, rep);
    }
  });
});

function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function (err, replies) {
    if (err) {
      log.info("multi err: " + err);
      rep({ code: 500, msg: err.message });
    } else {
      rep({ code: 200, data: replies });
    }
  });
}

log.info("Start server at %s and connect to %s", config.svraddr, config.msgaddr);

svc.run();
