import { Processor, Config, ModuleFunction, DoneFunction, rpc } from "hive-processor";
import { Client as PGClient, ResultSet } from "pg";
import { createClient, RedisClient } from "redis";
import { servermap, triggermap } from "hive-hostmap";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";

let log = bunyan.createLogger({
  name: "profile-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/profile-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/profile-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  cachehost: process.env["CACHE_HOST"],
  addr: "ipc:///tmp/profile.ipc"
};

let user_trigger = nanomsg.socket("pub");
user_trigger.bind(triggermap.group);


let processor = new Processor(config);

processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction) => {
  log.info("refresh");
  db.query("SELECT id, openid, name, gender, identity_no, phone, nickname, portrait FROM users", [], (err: Error, result: ResultSet) => {
    if (err) {
      log.error(err, "select users from db error");
      return;
    } else {
      let users = result.rows.map(row => row2user(row));
      let multi = cache.multi();
      for (let user of users) {
        multi.hset("profile-entities", user.id, JSON.stringify(user));
        multi.lpush("profile", user.id);
      }
      multi.exec((err, replies) => {
        if (err) {
          log.info("multi err" + err);
        }
        done();
      });
    }
  });
});

processor.call("addUserInfo", (db: PGClient, cache: RedisClient, done: DoneFunction, uid: string, openid: string, gender: string, nickname: string, portrait: string, callback: string) => {
  log.info(`addUserInfo openid: ${openid}, gender: ${gender}, nickname: ${nickname}, portrait: ${portrait}, callback: ${callback}`);
  db.query("INSERT INTO users (id, openid, gender, nickname, portrait) VALUES ($1, $2, $3, $4 ,$5)", [uid, openid, gender, nickname, portrait], (err: Error) => {
    if (err) {
      log.error(err, "insert into DB 'users' error!");
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
    } else {
      let profile_entities = { id: uid, openid: openid, gender: gender, nickname: nickname, portrait: portrait };
      let multi = cache.multi();
      multi.hset("profile-entities", uid, JSON.stringify(profile_entities));
      multi.lpush("profile", uid);
      multi.setex(callback, 30, JSON.stringify({
        code: 200,
        data: openid
      }));
      multi.exec((err, replies) => {
        if (err) {
          log.error(err, "cache multi error!");
          cache.setex(callback, 30, JSON.stringify({
            code: 500,
            msg: err.message
          }));
        }
        done();
      });
      // user_trigger.send(msgpack.encode({ uid }));
    }
  });
});

function row2user(row) {
  return {
    id: row.id,
    openid: row.openid ? row.openid.trim() : "",
    name: row.name ? row.name.trim() : "",
    gender: row.gender ? row.gender.trim() : "",
    identity_no: row.identity_no ? row.identity_no.trim() : "",
    phone: row.phone ? row.phone.trim() : "",
    nickname: row.nickname ? row.nickname.trim() : "",
    portrait: row.portrait ? row.portrait.trim() : "",
  };
}

log.info("Start processor at %s", config.addr);
processor.run();
