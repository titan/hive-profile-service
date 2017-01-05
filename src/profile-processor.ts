import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as bluebird from "bluebird";

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

export const processor = new Processor();

async function sync_users(db: PGClient, cache: RedisClient, uid?: string): Promise<any> {
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  if (!uid) {
    multi.del("profile-entities");
    multi.del("profile-uid");
    multi.del("profile");
    multi.del("wxuser");
    multi.del("openid_ticket");
  }
  const result = await db.query("SELECT id, openid, name, gender, identity_no, phone, nickname, portrait, pnrid, ticket, created_at, updated_at, tender_opened FROM users" + (uid ? " WHERE id = $1" : ""), uid ? [uid] : []);
  const users = [];
  for (const row of result.rows) {
    users.push(row2user(row));
  }
  for (const user of users) {
    const pkt = await msgpack_encode(user);
    multi.hset("profile-entities", user["id"], pkt);
    multi.hset("pnrid-uid", user["pnrid"], user["id"]);
    multi.hset("wxuser", user["id"], user["openid"]);
    multi.hset("wxuser", user["openid"], user["id"]);
  }
  return multi.execAsync();
}

processor.call("refresh", (ctx: ProcessorContext, cbflag: string, uid?: string) => {
  log.info(`refresh cbflag: ${cbflag}${uid ? ", uid: " + uid : "" }`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      await sync_users(db, cache, uid);
      await set_for_response(cache, cbflag, { code: 200, data: "success" });
      done();
    } catch (e) {
      log.error(e);
      done();
      set_for_response(cache, cbflag, { code: 500, msg: e.message });
    }
  })();
});

processor.call("setTenderOpened", (ctx: ProcessorContext, flag: boolean, cbflag: string, uid: string) => {
  log.info(`setTenderOpened flag: ${flag}, cbflag: ${cbflag}, uid: ${uid}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      const result = await db.query("SELECT count(1) FROM users WHERE id = $1", [uid]);
      if (result.rows[0].count == 0) {
        await set_for_response(cache, cbflag, { code: 404, msg: "User not found" });
        done();
        return;
      }
      await db.query("UPDATE users SET tender_opened = $1 WHERE id = $2", [flag, uid]);
      await sync_users(db, cache, uid);
      await set_for_response(cache, cbflag, { code: 200, data: "success" });
      done();
    } catch (e) {
      log.error(e);
      done();
      set_for_response(cache, cbflag, { code: 500, msg: e.message });
    }
  })();
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
    pnrid: row.pnrid ? row.pnrid.trim() : "",
    ticket: row.ticket ? row.ticket.trim() : "",
    created_at: row.created_at,
    updated_at: row.updated_at,
    tender_opened: row.tender_opened
  };
}
