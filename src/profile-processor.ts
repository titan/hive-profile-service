import { BusinessEventContext, Processor, ProcessorContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waiting, msgpack_decode_async, msgpack_encode_async, Result } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { User } from "profile-library";
import { Person } from "person-library";
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
  const result = await db.query("SELECT id, openid, name, password, gender, identity_no, phone, nickname, portrait, pnrid, ticket, inviter, created_at, updated_at, tender_opened, insured, max_orders, disabled FROM users" + (uid ? " WHERE id = $1" : ""), uid ? [uid] : []);
  const users: User[] = [];
  for (const row of result.rows) {
    users.push(row2user(row));
  }
  for (const user of users) {
    multi.hset("pnrid-uid", user.pnrid, user.id);
    multi.hset("wxuser", user.id, user.openid);
    multi.hset("wxuser", user.openid, user.id);
    user.openid = "";
    const pkt = await msgpack_encode_async(user);
    multi.hset("profile-entities", user.id, pkt);
  }
  return multi.execAsync();
}

processor.callAsync("refresh", async (ctx: ProcessorContext, uid?: string) => {
  log.info(`refresh ${uid || ""}`);
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

processor.callAsync("setInsured", async (ctx: ProcessorContext, insured: string) => {
  log.info(`setInsured, uid: ${ctx.uid}, insured: ${insured} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const uid = ctx.uid;
  const prep: Result<Person> = await rpcAsync<Person>(ctx.domain, process.env["PERSON"], ctx.uid, "getPerson", insured);
  if (prep.code === 200) {
    const insured_person: Person = prep.data;
    const uresult = await db.query("SELECT id FROM users WHERE insured = $1", [insured]);
    if (uresult.rowCount > 1) {
      if (uid !== uresult.rows[0].id) {
        if (insured_person.verified) {
          const err = new Error("501");
          err.message = `绑定互助会员失败(pcb501)，该互助会员(${insured})已在其他微信号上绑定！`;
          ctx.report(0, err);
          return { code: 501, msg: "绑定互助会员失败(pcb501)，该互助会员助已在其他微信号上绑定！" };
        } else {
          await db.query("BEGIN");
          await db.query("UPDATE users SET insured = $1 WHERE id = $2", [insured, uid]);
          await db.query("UPDATE users SET insured = NULL WHERE id = $1", [uresult.rows[0].id]);
          await db.query("COMMIT");
          await sync_users(db, cache, uid);
          return { code: 200, data: insured };
        }
      } else {
        return { code: 200, data: insured };
      }
    } else {
      await db.query("UPDATE users SET insured = $1 WHERE id = $2", [insured, uid]);
      await sync_users(db, cache, uid);
      return { code: 200, data: insured };
    }
  } else {
    return { code: 404, msg: "互助会员不存在" };
  }
});

processor.callAsync("setTenderOpened", async (ctx: ProcessorContext, flag: boolean, uid: string) => {
  log.info(`setTenderOpened flag: ${flag}, uid: ${uid}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const result = await db.query("SELECT 1 FROM users WHERE id = $1", [uid]);
    if (result.rowCount === 0) {
      return { code: 404, msg: "未找到对应用户" };
    } else {
      await db.query("UPDATE users SET tender_opened = $1 WHERE id = $2", [flag, uid]);
      await sync_users(db, cache, uid);
      return { code: 200, data: "success" };
    }
  } catch (e) {
    ctx.report(0, e);
    log.error(e);
    throw { code: 500, msg: e.message };
  }
});

processor.callAsync("getRecommend", async (ctx: ProcessorContext, uid: string) => {
  log.info(`getRecommend, uid: ${uid}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const result = await db.query("SELECT DISTINCT ut.uid FROM user_tickets AS ut INNER JOIN users AS u on u.ticket = ut.ticket WHERE u.id = $1", [uid]);
  if (result.rowCount === 0) {
    return { code: 404, msg: "未找到用户的推荐人" };
  } else {
    const recommend = result.rows[0].uid;
    const pkt = await cache.hgetAsync("profile-entities", recommend);
    const profile = await msgpack_decode_async(pkt);
    return { code: 200, data: profile };
  }
});

function row2user(row): User {
  return {
    id: row.id,
    name: row.name ? row.name.trim() : "",
    password: row.password ? row.password.trim() : "",
    openid: row.openid ? row.openid.trim() : "",
    gender: row.gender ? row.gender.trim() : "",
    identity_no: row.identity_no ? row.identity_no.trim() : "",
    phone: row.phone ? row.phone.trim() : "",
    nickname: row.nickname ? row.nickname.trim() : "",
    portrait: row.portrait ? row.portrait.trim() : "",
    pnrid: row.pnrid ? row.pnrid.trim() : "",
    ticket: row.ticket ? row.ticket.trim() : "",
    inviter: row.inviter ? row.inviter.trim() : "",
    created_at: row.created_at,
    updated_at: row.updated_at,
    tender_opened: row.tender_opened,
    insured: row.insured,
    max_orders: row.max_orders,
    disabled: row.disabled,
  };
}
