import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
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

processor.call("refresh", (ctx: ProcessorContext, callback: string) => {
  log.info("refresh");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      const result = await db.query("SELECT id, openid, name, gender, identity_no, phone, nickname, portrait, pnrid, ticket, created_at, updated_at FROM users");
      let users = [];
      for (let row of result.rows) {
        users.push(row2user(row));
      }
      let multi = bluebird.promisifyAll(cache.multi()) as Multi;
      for (let user of users) {
        let pkt = await msgpack_encode(user);
        multi.hset("profile-entities", user["id"], pkt);
        multi.hset("pnrid-uid", user["pnrid"], user["id"]);
        multi.lpush("profile", user["id"]);
        multi.hset("wxuser", user["id"], user["openid"]);
        multi.hset("wxuser", user["openid"], user["id"]);
        let ticket_entities = {
          openid: user["openid"],
          ticket: user["ticket"],
          CreateTime: user["updated_at"]
        };
        let pkt2 = await msgpack_encode(ticket_entities);
        multi.hset("openid_ticket", user["openid"], pkt2);
      }
      await multi.execAsync();
    } catch (e) {
      log.error(e);
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
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
  };
}