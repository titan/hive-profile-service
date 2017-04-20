import { BusinessEventContext, Processor, ProcessorContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waiting, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
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
  const result = await db.query("SELECT id, openid, name, gender, identity_no, phone, nickname, portrait, pnrid, ticket, inviter, created_at, updated_at, tender_opened, insured, max_orders FROM users" + (uid ? " WHERE id = $1" : ""), uid ? [uid] : []);
  const users = [];
  for (const row of result.rows) {
    users.push(row2user(row));
  }
  for (const user of users) {
    multi.hset("pnrid-uid", user["pnrid"], user["id"]);
    multi.hset("wxuser", user["id"], user["openid"]);
    multi.hset("wxuser", user["openid"], user["id"]);
    delete user["openid"];
    const pkt = await msgpack_encode(user);
    multi.hset("profile-entities", user["id"], pkt);
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

async function checkInsured(insured, user): Promise<any> {
  log.info(`checkInsured`);
  try {
    const old_insured = user["insured"];
    const uid = user["id"];
    if (old_insured !== null && old_insured !== undefined && old_insured !== "") {
      if (old_insured !== insured) {
        const prep = await rpcAsync("mobile", process.env["PERSON"], uid, "getPerson", old_insured);
        if (prep["code"] === 200) {
          if (prep["data"]["verified"] === true) {
            return { code: 200, data: true, insured: old_insured };
          } else {
            return { code: 200, data: false };
          }
        } else {
          return { code: 200, data: false };
        }
      } else {
        return { code: 200, data: true, insured: old_insured };
      }
    } else {
      return { code: 200, data: false };
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

processor.callAsync("setInsured", async (ctx: ProcessorContext, user: Object, insured: string) => {
  log.info(`setInsured,uid: ${ctx.uid},user:${JSON.stringify(user)} `);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const uid = ctx.uid;
  try {
    const prep = await rpcAsync(ctx.domain, process.env["PERSON"], ctx.uid, "getPerson", insured);
    if (prep["code"] === 200) {
      if (prep["data"]["verified"] === true) {
        const orep = await rpcAsync(ctx.domain, process.env["ORDER"], ctx.uid, "getInsuredUid", insured);
        if (orep["code"] === 200) {
          const insured_uid = orep["data"];
          if (insured_uid !== ctx.uid) {
            const err = new Error(`绑定互助会员失败(pcb501)，该互助会员助已在其他微信号上绑定！`);
            ctx.report(0, err)
            return { code: 501, msg: "绑定互助会员失败(pcb501)，该互助会员助已在其他微信号上绑定！" };
          } else {
            const result = await checkInsured(insured, user);
            if (result["code"] === 200 && result["data"] === true) {
              return { code: 200, data: result["insured"] };
            } else if (result["code"] === 200 && result["data"] === false) {
              await db.query("UPDATE users SET insured = $1 WHERE id = $2", [insured, uid]);
              await sync_users(db, cache, uid);
              return { code: 200, data: insured };
            } else {
              return { code: result["code"], msg: result["msg"] };
            }
          }
        } else {
          const result = await checkInsured(insured, user);
          if (result["code"] === 200 && result["data"] === true) {
            return { code: 200, data: result["insured"] };
          } else if (result["code"] === 200 && result["data"] === false) {
            await db.query("UPDATE users SET insured = $1 WHERE id = $2", [insured, uid]);
            await sync_users(db, cache, uid);
            return { code: 200, data: insured };
          } else {
            return { code: result["code"], msg: result["msg"] };
          }
        }
      } else {
        const result = await checkInsured(insured, user);
        if (result["code"] === 200 && result["data"] === true) {
          return { code: 200, data: result["insured"] };
        } else if (result["code"] === 200 && result["data"] === false) {
          await db.query("UPDATE users SET insured = $1 WHERE id = $2", [insured, uid]);
          await sync_users(db, cache, uid);
          return { code: 200, data: insured };
        } else {
          return { code: result["code"], msg: result["msg"] };
        }
      }
    } else {
      log.info("获取互助会员信息失败：" + prep["msg"]);
      return { code: prep["code"], msg: prep["msg"] };
    }
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
    name: row.name ? row.name.trim() : "",
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
  };
}
