import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'profile-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/queue.ipc"
};

let processor = new Processor(config);

processor.call('refresh', (db: PGClient, cache: RedisClient, done: DoneFunction) => {
  log.info('refresh');
  db.query('SELECT id, openId, name, gender, identity_no, phone, nickname, portrait FROM users', [], (err: Error, result: ResultSet) => {
    if (err) {
      log.error(err, 'query error');
      return;
    }
    let users = [];
    for (let row of result.rows) {
      users.push(row2user(row));
    }
    console.log("users:"+users)
    let multi = cache.multi();
    for (let user of users) {
      multi.hset("profile-entities", user.id, JSON.stringify(user));
    }
    for (let user of users) {
      multi.lpush("profile", user.id);
    }
    multi.exec((err, replies) => {
    if (err) {
      console.error(err);
    }
    done(); // close db and cache connection
  });
    
  });
});

processor.call('setUserInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, uid, openid, gender, nickname, portrait) => {
  log.info('setUserInformation');
  db.query('INSERT INTO profile (id, openid, gender, nickname, portrait) VALUES ($1, $2, $3, $4 ,$5)',[uid, openid, gender, nickname, portrait], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     } else {
        let profile_entities = {id:uid, openid:openid, gender:gender, nickname:nickname, portrait:portrait};
        let multi = cache.multi();
        multi.hset("profile-entities", uid, JSON.stringify(profile_entities));
        multi.sadd("profile", uid);
        multi.exec((err, replies) => {
          if (err) {
            log.error(err);
          }
          done();
        });
      }
   });
});

function row2user(row) {
  return {
    id: row.id,
    openId: row.openId,
    name: row.name? row.name.trim(): '',
    gender: row.gender? row.gender.trim(): '',
    identity_no: row.identity_no,
    phone: row.phone,
    nickname: row.nickname? row.nickname.trim(): '',
    portrait: row.portrait? row.portrait.trim(): '',
  };
}

log.info('Start processor at %s', config.addr);
processor.run();

