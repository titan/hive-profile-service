import { Server, ServerContext, rpc, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode, msgpack_encode } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, numberVerifier, booleanVerifier } from "hive-verify";
import * as uuid from "node-uuid";

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

const tickets = [
  "gQH57zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1REbklHUDdtOGJHSV9pS3k4QldyAAIEMGApWAMEAAAAAA==",
  "gQHa8DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xLzN6bDFNQ1BtVExFMW5MR2FUUldyAAIE1ugfWAMEAAAAAA==",
  "gQFw7zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL0pqbFZKZ1BtUzdFeVUwaXJiUldyAAIELnEdWAMEAAAAAA==",
  "gQFw7zoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL0pqbFZKZ1BtUzdFeVUwaXJiUldyAAIELnEdWAMEAAAAAA==",
  "gQE78DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1BUbGtSc1RtUkxFOTcxUDFYQldyAAIEMFEQWAMEAAAAAA==",
  "gQFA8DoAAAAAAAAAASxodHRwOi8vd2VpeGluLnFxLmNvbS9xL1l6a0RDb1htRHJGM3ZBMlVPeFdyAAIEJ9z9VwMEAAAAAA==",
  "gQFd8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyeHpoQTVxTGpjUzIxMDAwMHcwM1gAAgTpI2ZYAwQAAAAA",
  "gQEK8TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyVzlZODV0TGpjUzIxMDAwMHcwM00AAgTDHWdYAwQAAAAA",
  "gQHe8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyRE5zaDRFTGpjUzIxMDAwME0wM0IAAgQHHmdYAwQAAAAA",
  "gQG98DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyaDdOcDRPTGpjUzIxMDAwME0wMzkAAgQ3HmdYAwQAAAAA",
  "gQHs8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyRjE5UDRPTGpjUzIxMDAwME0wM0EAAgRdHmdYAwQAAAAA",
  "gQEw8TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyVkJuYjVvTGpjUzIxMDAwMGcwMzQAAgSDHmdYAwQAAAAA",
  "gQHl8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyanJkUzVMTGpjUzIxMDAwMGcwM1cAAgSjHmdYAwQAAAAA",
  "gQE88TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyR0MzRTVRTGpjUzIxMDAwMGcwM1kAAgTWHmdYAwQAAAAA",
  "gQFU8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyQm11UTVmTGpjUzIxMDAwMGcwM3cAAgQXH2dYAwQAAAAA",
  "gQGr8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyeFBjODRGTGpjUzIxMDAwMDAwM2UAAgQ8H2dYAwQAAAAA",
  "gQFb8TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyS2hYYTVtTGpjUzIxMDAwMDAwMy0AAgSTH2dYAwQAAAAA",
  "gQHz8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyUUFTWDVkTGpjUzIxMDAwMHcwM18AAgS6H2dYAwQAAAAA",
  "gQGN8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAydGRXcDRqTGpjUzIxMDAwMGcwM0MAAgTlH2dYAwQAAAAA",
  "gQHC8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyX0ZNMDV2TGpjUzIxMDAwMHcwM0oAAgQIIGdYAwQAAAAA",
  "gQFG8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyc25zNjV4TGpjUzIxMDAwMGcwM0UAAgQvIGdYAwQAAAAA",
  "gQF_8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyWlFBNzRhTGpjUzIxMDAwMHcwM04AAgRdIGdYAwQAAAAA",
  "gQE38TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyYUI3YjRXTGpjUzIxMDAwMGcwM2oAAgSJIGdYAwQAAAAA",
  "gQEk8TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyamNhTDRjTGpjUzIxMDAwMDAwM0UAAgSuIGdYAwQAAAAA",
  "gQH48DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyQ3hVMzRLTGpjUzIxMDAwMGcwM2UAAgTSIGdYAwQAAAAA",
  "gQHp8DwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAybjdIRzVKTGpjUzIxMDAwMHcwM0wAAgT5IGdYAwQAAAAA",
  "gQHw7zwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyUzN4TzVfTGpjUzIxMDAwMGcwM2kAAgRWcHhYAwQAAAAA",
  "1089",
  "申健",
  "曲淼",
  "满强",
  "赵洋",
  "靳建行",
  "祁畅",
  "董盛瑞",
  "叶昌晶",
  "秦佳华"
];


server.callAsync("getUser", allowAll, "获得用户信息", "获得当前用户信息", async (ctx: ServerContext, uid?: string) => {
  log.info(`getUser, uid: ${uid ? uid : ctx.uid}`);
  try {
    const prep = await ctx.cache.hgetAsync("profile-entities", uid ? uid : ctx.uid);
    if (prep !== null && prep !== "") {
      const profile_entities = msgpack_decode(prep);
      return { code: 200, data: profile_entities };
    } else {
      return { code: 404, msg: "未找到对应用户信息" };
    }
  } catch (e) {
    log.info(e);
    throw { code: 500, msg: e.message };
  }
});


// server.call("getDiscountStatus", allowAll, "获得当前用户优惠情况", "获得当前用户优惠情况", (ctx: ServerContext, rep: ((result: any) => void), recommend: string) => {
//   log.info("getDiscountStatus " + ctx.uid);
//   ctx.cache.hget(entity_key, ctx.uid, function (err, result) {
//     if (err) {
//       rep({ code: 500, msg: err.message });
//     } else if (result) {
//       (async () => {
//         try {
//           const user = await msgpack_decode(result);
//           if (user["ticket"] && user["ticket"] !== "") {
//             for (let ticket of tickets) {
//               if (user["ticket"] === ticket) {
//                 rep({ code: 200, data: true });
//                 return;
//               } else if (recommend && recommend !== "") {
//                 if (recommend === ticket) {
//                   rep({ code: 200, data: true });
//                   return;
//                 }
//               }
//             }
//             rep({ code: 200, data: false });
//           } else if (recommend && recommend !== "") {
//             for (let ticket of tickets) {
//               if (recommend === ticket) {
//                 rep({ code: 200, data: true });
//                 return;
//               }
//             }
//             rep({ code: 200, data: false });
//           } else {
//             rep({ code: 200, data: false });
//           }
//         } catch (e) {
//           log.error(e);
//           rep({ code: 500, msg: e.message });
//         }
//       })();
//     } else {
//       rep({ code: 404, msg: "not found user" });
//     }
//   });
// });

server.callAsync("getInviter", allowAll, "获取邀请好友信息", "发送互助组邀请时使用", async (ctx: ServerContext, key: string) => {
  log.info(`getInviter, token: ${key}`);
  try {
    verify([stringVerifier("token", key)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const uid = await ctx.cache.getAsync("InviteKey:" + key);
    if (uid !== null && uid !== "") {
      const prep = await ctx.cache.hgetAsync("profile-entities", String(uid));
      if (prep !== null && prep !== "") {
        const profile_entities = await msgpack_decode(prep);
        return { code: 200, data: profile_entities };
      } else {
        return { code: 404, msg: "未找到对应用户信息" };
      }
    } else {
      return { code: 404, msg: "未找到对应用户信息" };
    }
  } catch (e) {
    log.info(e);
    throw { code: 500, msg: e.message };
  }
});


server.callAsync("getUserByUserIds", allowAll, "获取的用户信息", "获取一组用户信息", async (ctx: ServerContext, uids) => {
  log.info(`getUserByUserIds, uids: ${uids}`);
  try {
    verify([arrayVerifier("uids", uids)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const len = uids.length;
    if (len === 0) {
      return { code: 404, msg: "请选择需要查看的用户信息" };
    } else {
      const users = [];
      for (const uid of uids) {
        const prep = await ctx.cache.hgetAsync("profile-entities", uid);
        if (prep !== null && prep !== "") {
          const user = await msgpack_decode(prep);
          users.push(user);
        }
      }
      if (users.length === 0) {
        return { code: 404, msg: "未找到对应用户信息" };
      } else {
        const result = {};
        for (let user of users) {
          result[user.id] = user;
        }
        return { code: 200, data: result };
      }
    }
  } catch (e) {
    log.info(e);
    throw { code: 500, msg: e.message };
  }
});


server.callAsync("setInsured", allowAll, "获取投保人信息", "获取投保人信息", async (ctx: ServerContext, insured: string) => {
  try {
    verify([uuidVerifier("insured", insured)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = [ctx.uid, insured];
  const pkt: CmdPacket = { cmd: "setInsured", args: args };
  ctx.publish(pkt);
  waitingAsync(ctx);
});


server.callAsync("refresh", adminOnly, "refresh", "refresh", async (ctx: ServerContext, uid?: string) => {
  if (uid) {
    log.info(`refresh ${uid}`);
    try {
      verify([uuidVerifier("uid", uid)]);
    } catch (e) {
      log.info(e);
      return { code: 400, msg: e.message };
    }
  } else {
    log.info(`refresh`);
  }
  const args = uid ? [uid] : [];
  const pkt: CmdPacket = { cmd: "refresh", args: args };
  ctx.publish(pkt);
  waitingAsync(ctx);
});


server.callAsync("setTenderOpened", adminOnly, "设置开通自动投标标志", "设置开通自动投标标志", async (ctx: ServerContext, flag: boolean, uid?: string) => {
  if (uid) {
    log.info(`setTenderOpened, flag: ${flag}, uid: ${uid}`);
    try {
      verify([uuidVerifier("uid", uid), booleanVerifier("flag", flag)]);
    } catch (e) {
      log.info(e);
      return { code: 400, msg: e.message };
    }
  } else {
    log.info(`setTenderOpened, flag: ${flag}`);
    try {
      verify([booleanVerifier("flag", flag)]);
    } catch (e) {
      log.info(e);
      return { code: 400, msg: e.message };
    }
  }
  const args = uid ? [flag, uid] : [flag];
  const pkt: CmdPacket = { cmd: "setTenderOpened", args: args };
  ctx.publish(pkt);
  waitingAsync(ctx);
});

