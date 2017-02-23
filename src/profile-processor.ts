import { Processor, ProcessorContext, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode, msgpack_encode } from "hive-service";
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
  const result = await db.query("SELECT id, openid, name, gender, identity_no, phone, nickname, portrait, pnrid, ticket, created_at, updated_at, tender_opened, insured FROM users" + (uid ? " WHERE id = $1" : ""), uid ? [uid] : []);
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

processor.callAsync("refresh", async (ctx: ProcessorContext, uid?: string) => {
  log.info(`refresh${uid ? ", uid: " + uid : ""}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await sync_users(db, cache, uid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.error(e);
    throw { code: 500, msg: e.message };
  }
});

processor.callAsync("setInsured", async (ctx: ProcessorContext, uid: string, insured: string) => {
  log.info(`setInsured,uid: ${uid}, insured: ${insured}`);
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    await db.query("UPDATE SET insured = $1 WHERE id = $2", [insured, uid]);
    await sync_users(db, cache, uid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
});

processor.callAsync("setTenderOpened", async (ctx: ProcessorContext, flag: boolean, uid: string) => {
  log.info(`setTenderOpened flag: ${flag}, uid: ${uid}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const result = await db.query("SELECT count(1) FROM users WHERE id = $1", [uid]);
    if (result.rows[0].count === 0) {
      return { code: 404, msg: "未找到对应用户" };
    } else {
      await db.query("UPDATE users SET tender_opened = $1 WHERE id = $2", [flag, uid]);
      await sync_users(db, cache, uid);
      return { code: 200, data: "success" };
    }
  } catch (e) {
    log.error(e);
    throw { code: 500, msg: e.message };

  }
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
    tender_opened: row.tender_opened,
    insured: row.insured
  };
}
