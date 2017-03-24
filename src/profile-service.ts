import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./profile-server";
import { processor } from "./profile-processor";
import * as bunyan from "bunyan";

const log = bunyan.createLogger({
  name: "profile-service",
  streams: [
    {
      level: "info",
      path: "/var/log/profile-service-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/profile-service-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

const loginfo = (...x) => log.info(x);
const logerror = (...x) => log.error(x);

const config: Config = {
  modname: "PROFILE",
  serveraddr: process.env["PROFILE"],
  queueaddr: "ipc:///tmp/profile.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  loginfo: loginfo,
  logerror: logerror
};

const svc: Service = new Service(config);

svc.registerServer(server);
svc.registerProcessor(processor);

svc.run();

