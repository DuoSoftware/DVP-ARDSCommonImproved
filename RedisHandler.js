/**
 * Created by Heshan.i on 10/4/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var q = require('q');
var redis = require('ioredis');
var redisLock = require('redlock');
var config = require('config');

var redisIp = config.Redis.ip;
var redisPort = config.Redis.port;
var redisPass = config.Redis.password;
var redisMode = config.Redis.mode;
var redisDb = config.Redis.db;


//----------------Initiate Redis-------------------------------

var redisSetting =  {
    port:redisPort,
    host:redisIp,
    family: 4,
    password: redisPass,
    db: redisDb,
    retryStrategy: function (times) {
        return Math.min(times * 50, 2000);
    },
    reconnectOnError: function (err) {
        return true;
    }
};

if(redisMode == 'sentinel'){

    if(config.Redis.sentinels && config.Redis.sentinels.hosts && config.Redis.sentinels.port && config.Redis.sentinels.name){
        var sentinelHosts = config.Redis.sentinels.hosts.split(',');
        if(Array.isArray(sentinelHosts) && sentinelHosts.length > 2){
            var sentinelConnections = [];

            sentinelHosts.forEach(function(item){

                sentinelConnections.push({host: item, port:config.Redis.sentinels.port})

            });

            redisSetting = {
                sentinels:sentinelConnections,
                name: config.Redis.sentinels.name,
                password: redisPass,
                db: redisDb
            }

        }else{

            logger.error("Not enough sentinel servers found .........");
        }

    }
}

var client = undefined;

if(redisMode != "cluster") {
    client = new redis(redisSetting);
}else{

    var redisHosts = redisIp.split(",");
    if(Array.isArray(redisHosts)){


        redisSetting = [];
        redisHosts.forEach(function(item){
            redisSetting.push({
                host: item,
                port: redisPort,
                family: 4,
                password: redisPass,
                db: redisDb});
        });

        client = new redis.Cluster([redisSetting]);

    }else{

        client = new redis(redisSetting);
    }


}

var redLock = new redisLock([client], {driftFactor: 0.01, retryCount:  10000, retryDelay:  200});

client.on("error", function (err) {
    logger.error('Redis connection error :: %s', err);
});

client.on("connect", function () {
    logger.info("Connecting to Redis server success");
});

redLock.on('clientError', function(err) {
    logger.error('Initiate Redis-Lock failed :: %s', err);

});


//----------------Access Redis-------------------------------

//***Key***
var rSet = function (logKey, key, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SET :: key: %s :: value: %s', logKey, key, value);

        client.set(key, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSetNx = function (logKey, key, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SET NX :: key: %s :: value: %s', logKey, key, value);

        client.setnx(key, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SET NX failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SET NX success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SET NX failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rGet = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis GET :: key: %s', logKey, key);

        client.get(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis GET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis GET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis GET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rMGet = function (logKey, keys) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis MGET :: keys: %s :: value: %j', logKey, keys);

        client.mget(keys, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis MGET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis MGET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis MGET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rDel = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis DEL :: key: %s :: value: %s', logKey, key);

        client.del(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis GET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis GET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis GET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rIncr = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis INCR :: key: %s :: value: %s', logKey, key);

        client.incr(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis INCR failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis INCR success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis INCR failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rExists = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis Exists :: key: %s :: value: %s', logKey, key);

        client.exists(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis Exists failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis Exists success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis Exists failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

//***List***
var rRPush = function (logKey, key, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis RPush :: key: %s :: value: %s', logKey, key, value);

        client.rpush(key, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis RPush failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis RPush success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis RPush failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rLPush = function (logKey, key, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis LPush :: key: %s :: value: %s', logKey, key, value);

        client.lpush(key, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis LPush failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis LPush success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis LPush failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rLPop = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis LPop :: key: %s', logKey, key);

        client.lpop(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis LPop failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis LPop success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis LPop failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rLLen = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis LLen :: key: %s', logKey, key);

        client.llen(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis LLen failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis LLen success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis LLen failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rLRange = function (logKey, key, start, stop) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis LRange :: key: %s :: start: %s :: stop: %s', logKey, key, start, stop);

        client.lrange(key, start, stop, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis LRange failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis LRange success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis LRange failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rLRem = function (logKey, key, count, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis LRem :: key: %s :: count: %s :: value: %s', logKey, key, count, value);

        client.lrem(key, count, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis LRem failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis LRem success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis LRem failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

//***Hash***
var rHSet = function (logKey, key, field, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HSET :: key: %s ::  field: %s :: value: %s', logKey, key, field, value);

        client.hset(key, field, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HSET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HSET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HSET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rHSetNx = function (logKey, key, field, value) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HSETNX :: key: %s ::  field: %s :: value: %s', logKey, key, field, value);

        client.hsetnx(key, field, value, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HSETNX failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HSETNX success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HSETNX failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rHGet = function (logKey, key, field) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HGET :: key: %s ::  field: %s', logKey, key, field);

        client.hget(key, field, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HGET failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HGET success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HGET failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rHDel = function (logKey, key, field) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HDEL :: key: %s ::  field: %s', logKey, key, field);

        client.hdel(key, field, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HDEL failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HDEL success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HDEL failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rHExists = function (logKey, key, field) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HExists :: key: %s ::  field: %s', logKey, key, field);

        client.hexists(key, field, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HExists failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HExists success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HExists failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rHVals = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis HVALS :: key: %s :: value: %s', logKey, key);

        client.hvals(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis HVALS failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis HVALS success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis HVALS failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

//***Set***
var rSAdd = function (logKey, key, member) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SADD :: key: %s :: member: %s', logKey, key, member);

        client.sadd(key, member, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SADD failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SADD success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SADD failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSMembers = function (logKey, key) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SMEMBERS :: key: %s', logKey, key);

        client.smembers(key, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SMEMBERS failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SMEMBERS success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SMEMBERS failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSRem = function (logKey, key, member) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SREM :: key: %s :: member: %s', logKey, key, member);

        client.srem(key, member, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SREM failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SREM success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SREM failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSIsMember = function (logKey, key, member) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SISMEMBER :: key: %s :: member: %s', logKey, key, member);

        client.sismember(key, member, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SISMEMBER failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SISMEMBER success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SISMEMBER failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSInter = function (logKey, keys) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SINTER :: keys: %j', logKey, keys);

        client.sinter(keys, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SINTER failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SINTER success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SINTER failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var rSMove = function (logKey, source, destination, member) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis SMOVE :: source: %s :: destination: %s :: member: %s', logKey, source, destination, member);

        client.smove(source, destination, member, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis SMOVE failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis SMOVE success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis SMOVE failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


//***Pub/Sub***
var rPublish = function (logKey, pattern, message) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis Publish :: pattern: %s :: message: %s', logKey, pattern, message);

        client.publish(pattern, message, function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis Publish failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis Publish success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis Publish failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


//***Pipeline***
var rPipeline  = function (logKey, commands) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Redis Pipeline :: commands: %j', logKey, commands);

        client.pipeline(commands).exec(function (err, result) {
            if(err){
                logger.error('LogKey: %s - Redis Pipeline failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                logger.info('LogKey: %s - Redis Pipeline success :: %s', logKey, result);
                deferred.resolve(result);
            }
        });
    }catch(ex){
        logger.error('LogKey: %s - Redis Pipeline failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

module.exports.R_Set = rSet;
module.exports.R_SetNx = rSetNx;
module.exports.R_Get = rGet;
module.exports.R_MGet = rMGet;
module.exports.R_Del = rDel;
module.exports.R_Incr = rIncr;
module.exports.R_Exists = rExists;

module.exports.R_RPush = rRPush;
module.exports.R_LPush = rLPush;
module.exports.R_LPop = rLPop;
module.exports.R_LLen = rLLen;
module.exports.R_LRange = rLRange;
module.exports.R_LRem = rLRem;

module.exports.R_HSet = rHSet;
module.exports.R_HSetNx = rHSetNx;
module.exports.R_HGet = rHGet;
module.exports.R_HDel = rHDel;
module.exports.R_HExists = rHExists;
module.exports.R_HVals = rHVals;

module.exports.R_SAdd = rSAdd;
module.exports.R_SMembers = rSMembers;
module.exports.R_SRem = rSRem;
module.exports.R_SIsMember = rSIsMember;
module.exports.R_SInter = rSInter;
module.exports.R_SMove = rSMove;

module.exports.R_Publish = rPublish;

module.exports.R_Pipeline = rPipeline;