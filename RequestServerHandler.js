/**
 * Created by Heshan.i on 10/24/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var redisHandler = require('./RedisHandler');
var tagHandler = require('./TagHandler');
var restClient = require('./RestClient');
var requestQueueAndStatusHandler = require('./RequestQueueAndStatusHandler');
var q = require('q');
var util = require('util');


var addRequestServer = function (logKey, tenant, company, requestServerData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - AddRequestServer :: tenant: %d :: company: %d :: requestServerData: %j', logKey, tenant, company, requestServerData);

        if (!requestServerData.QueuePositionCallbackUrl) {
            requestServerData.QueuePositionCallbackUrl = "";
            requestServerData.ReceiveQueuePosition = false;
        }
        if (!requestServerData.ReceiveQueuePosition) {
            requestServerData.ReceiveQueuePosition = false;
        }

        var requestServerKey = util.format('ReqServer:%s', requestServerData.ServerID);
        var requestServerTags = [
            'serverType_' + requestServerData.ServerType,
            'requestType_' + requestServerData.RequestType,
            'objType_RequestServer'
        ];

        redisHandler.R_Set(logKey, requestServerKey, JSON.stringify(requestServerData)).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - AddRequestServer - R_Set success :: %s', logKey, result);
            return tagHandler.SetTags(logKey, 'Tag:RequestServer', requestServerTags, requestServerKey);
        }).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - AddRequestServer - SetTags success :: %s', logKey, result);
            deferred.resolve('Add request server data success');

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - AddRequestServer - R_Set failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - AddRequestServer failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var setRequestServer = function (logKey, tenant, company, requestServerData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - SetRequestServer :: tenant: %d :: company: %d :: requestServerData: %j', logKey, tenant, company, requestServerData);

        if (!requestServerData.QueuePositionCallbackUrl) {
            requestServerData.QueuePositionCallbackUrl = "";
            requestServerData.ReceiveQueuePosition = false;
        }
        if (!requestServerData.ReceiveQueuePosition) {
            requestServerData.ReceiveQueuePosition = false;
        }

        var requestServerKey = util.format('ReqServer:%s', requestServerData.ServerID);
        var requestServerTags = [
            'serverType_' + requestServerData.ServerType,
            'requestType_' + requestServerData.RequestType,
            'objType_RequestServer'
        ];

        tagHandler.RemoveTags(logKey, requestServerKey).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - SetRequestServer - RemoveTags success :: %s', logKey, result);
            return redisHandler.R_Set(logKey, requestServerKey, JSON.stringify(requestServerData));
        }).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - SetRequestServer - R_Set success :: %s', logKey, result);
            return tagHandler.SetTags(logKey, 'Tag:RequestServer', requestServerTags, requestServerKey);
        }).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - SetRequestServer - SetTags success :: %s', logKey, result);
            deferred.resolve('Add request server data success');

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - SetRequestServer - R_Set failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - SetRequestServer failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getRequestServer = function (logKey, tenant, company, requestServerId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - GetRequestServer :: tenant: %d :: company: %d :: requestServerId: %s', logKey, tenant, company, requestServerId);

        var requestServerKey = util.format('ReqServer:%s', requestServerId);

        redisHandler.R_Get(logKey, requestServerKey).then(function (result) {

            if (result) {

                logger.info('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get success :: %s', logKey, result);
                deferred.resolve(JSON.parse(result));
            } else {

                logger.info('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get No request server data found', logKey);
                deferred.reject('No request server data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - GetRequestServer failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getRequestServerByType = function (logKey, tenant, company, serverType, requestType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - GetRequestServer :: tenant: %d :: company: %d :: serverType: %s :: requestType: %s', logKey, tenant, company, serverType, requestType);

        var requestServerSearchTags = [
            'Tag:RequestServer:serverType_' + serverType,
            'Tag:RequestServer:requestType_' + requestType,
            'objType_RequestServer'
        ];

        redisHandler.R_SInter(logKey, requestServerSearchTags).then(function (result) {

            return redisHandler.R_MGet(logKey, result);

        }).then(function (results) {

            if (results) {

                logger.info('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get success :: %j', logKey, results);
                deferred.resolve(JSON.parse(results));
            } else {

                logger.info('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get No request server data found', logKey);
                deferred.reject('No request server data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - GetRequestServer - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - GetRequestServer failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeRequestServer = function (logKey, tenant, company, requestServerId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - RemoveRequestServer :: tenant: %d :: company: %d :: requestServerId: %s', logKey, tenant, company, requestServerId);


        var requestServerKey = util.format('ReqServer:%s', requestServerId);

        tagHandler.RemoveTags(logKey, requestServerKey).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - RemoveRequestServer - RemoveTags success :: %s', logKey, result);
            return redisHandler.R_Del(logKey, requestServerKey);

        }).then(function (result) {

            logger.info('LogKey: %s - RequestServerHandler - RemoveRequestServer - R_Del success :: %s', logKey, result);
            deferred.resolve('Remove request server success');

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - RemoveRequestServer - R_Set failed :: %s', logKey, ex);
            deferred.reject(ex);

        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - RemoveRequestServer failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


var sendRoutingCallback = function (logKey, callbackUrl, callbackOption, callbackData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - SendRoutingCallback :: callbackUrl: %s :: callbackOption: %s :: callbackData: %j', logKey, callbackUrl, callbackOption, callbackData);

        switch (callbackOption.toLowerCase()) {
            case 'get':
                var httpUrl = util.format('%s? %s', callbackUrl, JSON.stringify(callbackData));
                restClient.DoGetExternal(logKey, httpUrl).then(function (response) {

                    if (response.code == "503" || response.result.startsWith("-ERR")) {

                        logger.error('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoGetExternal failed externally', logKey);
                        deferred.resolve('re-addRequired');

                    } else if (response.code == "200") {

                        logger.info('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoGetExternal success', logKey);
                        deferred.resolve('setNextItem');

                    } else {

                        logger.info('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoGetExternal invalid response', logKey);
                        deferred.resolve('re-addRequired');

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoGetExternal failed :: %s', logKey, ex);
                    deferred.resolve('re-addRequired');
                });

                break;

            case 'post':
                restClient.DoPostExternal(logKey, callbackUrl, callbackData).then(function (response) {

                    if (response.code == "503" || response.result.startsWith("-ERR")) {

                        logger.error('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoPostExternal failed externally', logKey);
                        deferred.resolve('re-addRequired');

                    } else if (response.code == "200") {

                        logger.info('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoPostExternal success', logKey);
                        deferred.resolve('setNextItem');

                    } else {

                        logger.info('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoPostExternal invalid response', logKey);
                        deferred.resolve('re-addRequired');

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestServerHandler - SendRoutingCallback - DoPostExternal failed :: %s', logKey, ex);
                    deferred.resolve('re-addRequired');
                });
                break;

            default :
                break;
        }

    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - SendRoutingCallback failed :: %s', logKey, ex);
        deferred.resolve('re-addRequired');
    }

    return deferred.promise;
};

var sendPositionCallback = function (logKey, tenant, company, requestType, queueId, positionUrl, positionOption) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestServerHandler - SendPositionCallback :: tenant: %d :: company: %d :: requestType: %s :: queueId: %s :: positionUrl: %s :: positionOption: %s', logKey, tenant, company, requestType, queueId, positionUrl, positionOption);

        requestQueueAndStatusHandler.GetQueuePositions(logKey, tenant, company, requestType, queueId).then(function (queuePositionData) {

            switch (positionOption.toLowerCase()) {
                case 'get':
                    var httpUrl = util.format('%s? %s', positionUrl, JSON.stringify(queuePositionData));
                    restClient.DoGetExternal(logKey, httpUrl).then(function (response) {

                        logger.info('LogKey: %s - RequestServerHandler - SendPositionCallback - DoPostExternal success :: %s', logKey, response);
                        deferred.resolve('Send queue positions success');

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - RequestServerHandler - SendPositionCallback - DoGetExternal failed :: %s', logKey, ex);
                        deferred.reject('Send queue positions failed');
                    });

                    break;

                case 'post':
                    restClient.DoPostExternal(logKey, positionUrl, queuePositionData).then(function (response) {

                        logger.info('LogKey: %s - RequestServerHandler - SendPositionCallback - DoPostExternal success :: %s', logKey, response);
                        deferred.resolve('Send queue positions success');

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - RequestServerHandler - SendPositionCallback - DoPostExternal failed :: %s', logKey, ex);
                        deferred.reject('Send queue positions failed');
                    });
                    break;

                default :
                    break;
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestServerHandler - SendPositionCallback - GetQueuePositions failed :: %s', logKey, ex);
            deferred.reject('Send queue positions failed');
        });


    } catch (ex) {

        logger.error('LogKey: %s - RequestServerHandler - SendPositionCallback failed :: %s', logKey, ex);
        deferred.reject('Send queue positions failed');
    }

    return deferred.promise;
};


module.exports.AddRequestServer = addRequestServer;
module.exports.SetRequestServer = setRequestServer;
module.exports.GetRequestServer = getRequestServer;
module.exports.GetRequestServerByType = getRequestServerByType;
module.exports.RemoveRequestServer = removeRequestServer;

module.exports.SendRoutingCallback = sendRoutingCallback;
module.exports.SendPositionCallback = sendPositionCallback;