import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as http from 'http';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';
import * as hostmap from './hostmap'

let log = bunyan.createLogger({
  name: 'profile-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/profile-server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/profile-server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, "redis"); // port, host

let list_key = "profile";
let entity_key = "profile-entities";
let wxuser_key = "wxuser";

let config: Config = {
  svraddr: hostmap.default["profile"],
  msgaddr: 'ipc:///tmp/profile.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

//获得当前用户信息
svc.call('getUserInfo', permissions, (ctx: Context, rep: ResponseFunction ) => {
  log.info('getUserInfo '+ ctx.uid);
    redis.hget(entity_key, ctx.uid, function (err, result) {
      if (err) {
        rep({code:500, msg:[]});
      } else {
        rep({code:200, user:JSON.parse(result)});
      }
    });
});

//获取某个用户的openid
svc.call('getUserOpenId', permissions, (ctx: Context, rep: ResponseFunction, uid:string) => {
  log.info('getUserInfo, ctx.uid:' + ctx.uid +  " arg uid:" + uid);
  redis.hget(wxuser_key, uid, function (err, result) {
    if (err) {
      rep({code:500, msg:[]});
    } else {
      rep({code:200, openid:result});
    }
  });
});

//添加用户信息
svc.call('addUserInfo', permissions, (ctx: Context, rep: ResponseFunction, openid:string, gender:string, nickname:string, portrait:string ) => {
  log.info('setUserInfo ' + ctx.uid);
  let args = {openid, gender, nickname, portrait}
  ctx.msgqueue.send(msgpack.encode({cmd: "addUserInfo", args: args}));
  log.info('addUserInfo' + args);
  rep({code:200, msg: 'sucessful'});
});


//refresh
svc.call('refresh', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('refresh ' + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
  rep({code:200, msg: 'sucessful'});
});

//获取所有用户信息
svc.call('getAllUsers', permissions, (ctx: Context, rep: ResponseFunction, start:number, limit:number) => {
  log.info('getAllUsers' + "uid is " + ctx.uid);
  redis.lrange(list_key, start, limit, function (err, result) {
    if (err) {
      rep({code:500, msg:[]});
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
  multi.exec(function(err, replies) {
    if (err) {
      log.info("multi err: " + err);
      rep({code:500, msg:[]});
    }else{
      rep({code:200, users:replies});
    }
  });
}

log.info('Start server at %s and connect to %s', config.svraddr, config.msgaddr);

svc.run();
