import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';
import * as uuid from 'node-uuid';

let log = bunyan.createLogger({
  name: 'vehicle-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, "redis"); // port, host

let list_key = "profile";
let entity_key = "profile-entities";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

svc.call('getUserInfo', permissions, (ctx: Context, rep: ResponseFunction, uids:string[]) => {
  log.info('getUserInfo %j', ctx);
  var users = {};
  for (let uid of uids) {
    let entity = redis.hget(entity_key, uid);
    users[uid] = entity;
  }
  rep(users);
});

svc.call('setUserInfo', permissions, (ctx: Context, rep: ResponseFunction, openid:string, gender:string, nickname:string, portrait:string ) => {
  log.info('setUserInfo %j', ctx);
  let uid = uuid.v1();
  let args = [uid, openid, gender, nickname, portrait]
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: args}));
});

svc.call('refresh', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('refresh %j', ctx);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
  rep({status: 'okay'});
});

function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    rep(replies);
  });
}

log.info('Start server at %s and connect to %s', config.svraddr, config.msgaddr);

svc.run();
