import { Server, ServerContext, rpcAsync, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async, msgpack_encode_async, Result } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, numberVerifier, booleanVerifier } from "hive-verify";
import * as bluebird from "bluebird";
import { User } from "profile-library";
import { Person } from "person-library";

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

let entity_key = "profile-entities";

export const server = new Server();

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];



server.callAsync("getUser", allowAll, "获得用户信息", "获得当前用户信息", async (ctx: ServerContext, uid?: string) => {
  const userid = uid || ctx.uid;
  log.info(`getUser, uid: ${userid}`);
  try {
    await verify([stringVerifier("uid", userid)]);
  } catch (e) {
    ctx.report(3, e);
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const prep = await ctx.cache.hgetAsync("profile-entities", uid ? uid : ctx.uid);
    if (prep !== null && prep !== "") {
      const profile_entities = await msgpack_decode_async(prep);
      return { code: 200, data: profile_entities };
    } else {
      return { code: 404, msg: "未找到对应用户信息" };
    }
  } catch (e) {
    ctx.report(0, e);
    log.info(e);
    return { code: 500, msg: e.message };
  }
});



server.callAsync("getInviter", allowAll, "获取邀请好友信息", "发送互助组邀请时使用", async (ctx: ServerContext, key: string) => {
  log.info(`getInviter, token: ${key}`);
  try {
    await verify([stringVerifier("token", key)]);
  } catch (e) {
    ctx.report(3, e);
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const uid = await ctx.cache.getAsync("InviteKey:" + key);
    if (uid !== null && uid !== "") {
      const prep = await ctx.cache.hgetAsync("profile-entities", String(uid));
      if (prep !== null && prep !== "") {
        const profile_entities = await msgpack_decode_async(prep);
        return { code: 200, data: profile_entities };
      } else {
        return { code: 404, msg: "未找到对应用户信息" };
      }
    } else {
      return { code: 404, msg: "未找到对应用户信息" };
    }
  } catch (e) {
    ctx.report(0, e);
    log.info(e);
    return { code: 500, msg: e.message };
  }
});


server.callAsync("getUsers", allowAll, "获取的用户信息", "获取一组用户信息", async (ctx: ServerContext, uids: string[]) => {
  log.info(`getUsers, uids: ${JSON.stringify(uids)}`);
  try {
    await verify([arrayVerifier("uids", uids)]);
  } catch (e) {
    ctx.report(3, e);
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const len = uids.length;
    if (len === 0) {
      return { code: 404, msg: "请选择需要查看的用户信息" };
    } else {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (const uid of uids) {
        multi.hget("profile-entities", uid);
      }
      const preps = await multi.execAsync();
      const users: User[] = [];
      for (const prep of preps.filter(x => x)) {
        const user: User = (await msgpack_decode_async(prep)) as User;
        users.push(user);
      }
      if (users.length === 0) {
        return { code: 404, msg: "未找到对应用户信息" };
      } else {
        const result = {};
        for (const user of users) {
          result[user.id] = user;
        }
        return { code: 200, data: result };
      }
    }
  } catch (e) {
    ctx.report(0, e);
    log.info(e);
    return { code: 500, msg: e.message };
  }
});


server.callAsync("getInsured", allowAll, "获取投保人信息", "获取投保人信息", async (ctx: ServerContext) => {
  log.info(`getInsured, uid:${ctx.uid}`);
  try {
    const urep = await ctx.cache.hgetAsync("profile-entities", ctx.uid);
    if (urep !== null && urep !== "") {
      const user: User = await msgpack_decode_async(urep) as User;
      const insured = user["insured"];
      if (insured) {
        const prep: Result<Person> = await rpcAsync<Person>("mobile", process.env["PERSON"], ctx.uid, "getPerson", insured);
        if (prep["code"] === 200) {
          return { code: 200, data: prep["data"] };
        } else {
          return { code: 404, msg: prep["msg"] };
        }
      } else {
        return { code: 404, msg: "未找到对应互助会员信息" };
      }
    } else {
      return { code: 404, msg: "未找到对应用户信息" };
    }
  } catch (e) {
    ctx.report(0, e);
    log.info(e);
    return { code: 500, msg: e.message };
  }
});

server.callAsync("setInsured", allowAll, "设置投保人信息", "设置投保人信息", async (ctx: ServerContext, insured: string) => {
  try {
    await verify([uuidVerifier("insured", insured)]);
  } catch (e) {
    ctx.report(3, e);
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const pkt: CmdPacket = { cmd: "setInsured", args: [insured]};
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});


server.callAsync("refresh", adminOnly, "refresh", "refresh", async (ctx: ServerContext, uid?: string) => {
  if (uid) {
    log.info(`refresh ${uid}`);
    try {
      await verify([uuidVerifier("uid", uid)]);
    } catch (e) {
      ctx.report(3, e);
      log.info(e);
      return { code: 400, msg: e.message };
    }
  } else {
    log.info(`refresh`);
  }
  const args = uid ? [uid] : [];
  const pkt: CmdPacket = { cmd: "refresh", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});


server.callAsync("setTenderOpened", allowAll, "设置开通自动投标标志", "设置开通自动投标标志", async (ctx: ServerContext, flag: boolean, uid: string) => {
  log.info(`setTenderOpened, flag: ${flag}, uid: ${uid ? uid : ctx.uid}`);
  if (uid) {
    if (ctx.domain === "admin") {
      try {
        await verify([uuidVerifier("uid", uid), booleanVerifier("flag", flag)]);
      } catch (e) {
        ctx.report(3, e);
        log.info(e);
        return { code: 400, msg: e.message };
      }
      const args = [flag, uid];
      const pkt: CmdPacket = { cmd: "setTenderOpened", args: args };
      ctx.publish(pkt);
      return await waitingAsync(ctx);
    } else {
      return { code: 403, msg: "暂无权限设置除本人外用户" };
    }
  } else {
    try {
      await verify([uuidVerifier("uid", ctx.uid), booleanVerifier("flag", flag)]);
    } catch (e) {
      ctx.report(3, e);
      log.info(e);
      return { code: 400, msg: e.message };
    }
    const args = [flag, ctx.uid];
    const pkt: CmdPacket = { cmd: "setTenderOpened", args: args };
    ctx.publish(pkt);
    return await waitingAsync(ctx);
  }
});

server.callAsync("getRecommend", allowAll, "获取新用户的推荐人", "获取新用户的推荐人（老用户）", async (ctx: ServerContext, uid?: string) => {
  log.info(`getRecommend, uid: ${ctx.domain === "mobile" ? ctx.uid : uid}`);
  const args = [];
  if (ctx.domain === "admin") {
    try {
      await verify([uuidVerifier("uid", uid)]);
    } catch (e) {
      ctx.report(3, e);
      log.error(e);
      return { code: 400, msg: e.message };
    }
    args.push(uid);
  } else {
    try {
      await verify([uuidVerifier("uid", ctx.uid)]);
    } catch (e) {
      ctx.report(3, e);
      log.error(e);
      return { code: 400, msg: e.message };
    }
    args.push(ctx.uid);
  }
  const pkt: CmdPacket = { cmd: "getRecommend", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

