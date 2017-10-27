/**
 * Created by Heshan.i on 10/24/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var redisHandler = require('./RedisHandler');
var rabbitMqHandler = require('./RabbitMqHandler');
var q = require('q');
var util = require('util');
var config = require('config');
var async = require('async');


//-------------------------Request Status----------------------------------------

var setRequestStatus = function (logKey, tenant, company, sessionId, status) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetRequestStatus :: tenant: %d :: company: %d :: sessionId: %s :: status: %s', logKey, tenant, company, sessionId, status);

        var requestStatusKey = util.format('RequestState:%d:%d:%s', tenant, company, sessionId);
        redisHandler.R_Set(logKey, requestStatusKey, status).then(function (result) {

            logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetRequestStatus - R_Set success', logKey);
            deferred.reject(result);

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetRequestStatus - R_Set failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetRequestStatus failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getRequestStatus = function (logKey, tenant, company, sessionId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - GetRequestStatus :: tenant: %d :: company: %d :: sessionId: %s', logKey, tenant, company, sessionId);

        var requestStatusKey = util.format('RequestState:%d:%d:%s', tenant, company, sessionId);
        redisHandler.R_Get(logKey, requestStatusKey).then(function (result) {

            logger.info('LogKey: %s - RequestQueueAndStatusHandler - GetRequestStatus - R_Get success', logKey);
            deferred.reject(result);

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - GetRequestStatus - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - GetRequestStatus failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeRequestStatus = function (logKey, tenant, company, sessionId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestStatus :: tenant: %d :: company: %d :: sessionId: %s', logKey, tenant, company, sessionId);

        var requestStatusKey = util.format('RequestState:%d:%d:%s', tenant, company, sessionId);
        redisHandler.R_Del(logKey, requestStatusKey).then(function (result) {

            logger.info('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestStatus - R_Del success', logKey);
            deferred.reject(result);

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestStatus - R_Del failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestStatus failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


//-------------------------Request Queue----------------------------------------

var setNextProcessingItem = function (logKey, queueId, processingHashId, currentSession) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem :: queueId: %s :: processingHashId: %s :: currentSession: %s', logKey, queueId, processingHashId, currentSession);

        redisHandler.R_HGet(logKey, processingHashId, queueId).then(function (processingHashSession) {

            if ((processingHashSession && processingHashSession == currentSession) || currentSession === "CreateHash") {

                var rejectedQueueId = util.format("%s:%s", queueId, "REJECTED");
                redisHandler.R_LPop(logKey, rejectedQueueId).then(function (nextRejectedItem) {

                    if (nextRejectedItem) {

                        redisHandler.R_HSet(logKey, processingHashId, queueId, nextRejectedItem).then(function (result) {

                            logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HSet: %s - queueId: %s - nextRejectedItem: %s success :: %s', logKey, processingHashId, rejectedQueueId, nextRejectedItem, result);
                            deferred.resolve('Process finished');

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HSet: %s - queueId: %s - nextRejectedItem: %s failed :: %s', logKey, processingHashId, rejectedQueueId, nextRejectedItem, ex);
                            deferred.resolve('Process finished');
                        });

                    } else {

                        redisHandler.R_LPop(logKey, queueId).then(function (nextItem) {

                            if (nextItem) {

                                redisHandler.R_HSet(logKey, processingHashId, queueId, nextItem).then(function (result) {

                                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HSet: %s - queueId: %s - nextRejectedItem: %s success :: %s', logKey, processingHashId, queueId, nextItem, result);
                                    deferred.resolve('Process finished');

                                }).catch(function (ex) {

                                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HSet: %s - queueId: %s - nextRejectedItem: %s failed :: %s', logKey, processingHashId, queueId, nextItem, ex);
                                    deferred.resolve('Process finished');
                                });

                            } else {

                                redisHandler.R_HDel(logKey, processingHashId, queueId).then(function (result) {

                                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HDel: %s - queueId: %s success :: %s', logKey, processingHashId, queueId, result);
                                    deferred.resolve('Process finished');

                                }).catch(function (ex) {

                                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HDel: %s - queueId: %s failed :: %s', logKey, processingHashId, queueId, ex);
                                    deferred.resolve('Process finished');

                                });

                            }

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_LPop: %s failed :: %s', logKey, queueId, ex);
                            deferred.resolve('Process finished');
                        });

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_LPop: %s failed :: %s', logKey, rejectedQueueId, ex);
                    deferred.resolve('Process finished');
                });

            } else {

                logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - Session Mismatched, Ignore SetNextItem', logKey);
                deferred.resolve('Process finished');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem - R_HGet failed :: %s', logKey, ex);
            deferred.resolve('Process finished');
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - SetNextProcessingItem failed :: %s', logKey, ex);
        deferred.resolve('Process finished');
    }

    return deferred.promise;
};

var addRequestToQueue = function (logKey, tenant, company, requestType, queueId, sessionId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue :: tenant: %d :: company: %d :: requestType: %s :: queueId: %s :: sessionId: %s', logKey, tenant, company, requestType, queueId, sessionId);

        redisHandler.R_RPush(logKey, queueId, sessionId).then(function (queuePosition) {

            if (queuePosition > 0) {

                logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - R_RPush success :: %d', logKey, queuePosition);

                var processingHashKey = util.format('ProcessingHash:%d:%d:%s', tenant, company, requestType);
                setRequestStatus(logKey, tenant, company, sessionId, 'QUEUED').then(function (result) {

                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - setRequestStatus success :: %s', logKey, result);

                    return redisHandler.R_HExists(logKey, processingHashKey, queueId);

                }).then(function (hashFieldExists) {

                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - R_HExists success :: %s', logKey, hashFieldExists);

                    var pubQueueId = request.QueueId.replace(/:/g, "-");
                    var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "ADDED", pubQueueId, "", sessionId);


                    if (hashFieldExists && hashFieldExists === 1) {

                        redisHandler.R_Publish(logKey, 'events', pubMessage);
                        deferred.resolve(queuePosition + 1);

                    } else {

                        var processingHashExist = false;
                        redisHandler.R_Exists(logKey, processingHashKey).then(function (hashExist) {
                            processingHashExist = (hashExist === 1);
                            setNextProcessingItem(logKey, queueId, processingHashKey, 'CreateHash');

                        }).then(function (setNextProcessingResult) {

                            logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - setNextProcessingItem success :: %s', logKey, setNextProcessingResult);

                            if (!processingHashExist && config.Host.UseMsgQueue === 'true')
                                rabbitMqHandler.Publish(logKey, 'ARDS.Workers.Queue', processingHashKey);

                            redisHandler.R_Publish(logKey, 'events', pubMessage);

                            deferred.resolve(queuePosition);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - setNextProcessingItem failed :: %s', logKey, ex);
                            deferred.reject('Set next processing item failed');
                        });

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue failed :: %s', logKey, ex);
                    deferred.reject('Add request to queue failed');
                });

            } else {

                logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - R_RPush failed :: %s', logKey, queuePosition);
                deferred.reject('Add request to queue failed');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue - R_RPush failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToQueue failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var addRequestToRejectQueue = function (logKey, tenant, company, requestType, queueId, sessionId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue :: tenant: %d :: company: %d :: requestType: %s :: queueId: %s :: sessionId: %s', logKey, tenant, company, requestType, queueId, sessionId);

        var rejectedQueueId = util.format("%s:%s", queueId, "REJECTED");
        redisHandler.R_RPush(logKey, rejectedQueueId, sessionId).then(function (queuePosition) {

            if (queuePosition > 0) {

                logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - R_RPush success :: %d', logKey, queuePosition);

                var processingHashKey = util.format('ProcessingHash:%d:%d:%s', tenant, company, requestType);
                setRequestStatus(logKey, tenant, company, sessionId, 'QUEUED').then(function (result) {

                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - setRequestStatus success :: %s', logKey, result);

                    return redisHandler.R_HExists(logKey, processingHashKey, queueId);

                }).then(function (hashFieldExists) {

                    logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - R_HExists success :: %s', logKey, hashFieldExists);

                    if (hashFieldExists && hashFieldExists === 1) {

                        deferred.resolve(queuePosition + 1);

                    } else {

                        var processingHashExist = false;
                        redisHandler.R_Exists(logKey, processingHashKey).then(function (hashExist) {
                            processingHashExist = (hashExist === 1);
                            setNextProcessingItem(logKey, queueId, processingHashKey, 'CreateHash');

                        }).then(function (setNextProcessingResult) {

                            logger.info('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - setNextProcessingItem success :: %s', logKey, setNextProcessingResult);

                            if (!processingHashExist && config.Host.UseMsgQueue === 'true') {

                                rabbitMqHandler.Publish(logKey, 'ARDS.Workers.Queue', processingHashKey);

                            }

                            deferred.resolve(queuePosition);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - setNextProcessingItem failed :: %s', logKey, ex);
                            deferred.reject('Set next processing item failed');
                        });

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue failed :: %s', logKey, ex);
                    deferred.reject('Add request to queue failed');
                });

            } else {

                logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - R_RPush failed :: %s', logKey, queuePosition);
                deferred.reject('Add request to queue failed');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue - R_RPush failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - AddRequestToRejectQueue failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeRequestFromQueue = function (logKey, tenant, company, requestType, queueId, sessionId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestFromQueue :: tenant: %d :: company: %d :: requestType: %s :: queueId: %s :: sessionId: %s', logKey, tenant, company, requestType, queueId, sessionId);

        redisHandler.R_LRem(logKey, queueId, 0, sessionId).then(function (removedItems) {

            var processingHashKey = util.format('ProcessingHash:%d:%d:%s', tenant, company, requestType);
            var pubQueueId = queueId.replace(/:/g, "-");
            var pubMessage = util.format("EVENT:%s:%s:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, "ARDS", "QUEUE", "REMOVED", pubQueueId, "", sessionId);
            var postAsyncTasks = [
                function (callback) {
                    redisHandler.R_Publish(logKey, 'events', pubMessage).then(function (result) {
                        callback(null, result);
                    }).catch(function (ex) {
                        callback(ex, null);
                    });
                },
                function (callback) {
                    redisHandler.R_HGet(logKey, processingHashKey, queueId).then(function (processingHashSession) {
                        if (processingHashSession && processingHashSession === sessionId) {
                            setNextProcessingItem(logKey, queueId, processingHashKey, sessionId).then(function (result) {
                                callback(null, result);
                            }).catch(function (ex) {
                                callback(ex, null);
                            });
                        } else {
                            callback(null, processingHashSession);
                        }
                    }).catch(function (ex) {
                        callback(ex, null);
                    });
                }
            ];

            if (removedItems && removedItems > 0) {

                async.parallel(async.reflectAll(postAsyncTasks), function () {
                    deferred.resolve('Remove request from queue success');
                });

            } else {

                var rejectedQueueId = util.format("%s:%s", queueId, "REJECTED");
                redisHandler.R_LRem(logKey, rejectedQueueId, 0, sessionId).then(function () {

                    async.parallel(async.reflectAll(postAsyncTasks), function () {
                        deferred.resolve('Remove request from queue success');
                    });

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestFromRejectQueue - R_LRem failed :: %s', logKey, ex);
                    deferred.reject(ex);
                });

            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestFromQueue - R_LRem failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - RemoveRequestFromQueue failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getQueuePositions = function (logKey, tenant, company, requestType, queueId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestQueueAndStatusHandler - GetQueuePositions :: tenant: %d :: company: %d :: requestType: %s :: queueId: %s', logKey, tenant, company, requestType, queueId);

        var positionData = [];
        var processingHashKey = util.format('ProcessingHash:%d:%d:%s', tenant, company, requestType);
        redisHandler.R_HGet(logKey, processingHashKey, queueId).then(function (processingSession) {

            if (processingSession)
                positionData.push({SessionId: processingSession, QueueId: queueId, QueuePosition: "1"});

            redisHandler.R_LRange(logKey, queueId, 0, -1).then(function (queuedSessions) {

                queuedSessions.forEach(function (item, i) {
                    if (item) {
                        var queuePosition = i + 2;
                        var requestPosition = {
                            SessionId: item,
                            QueueId: queueId,
                            QueuePosition: queuePosition.toString()
                        };
                        positionData.push(requestPosition);

                    }
                });

                deferred.resolve(positionData);

            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestQueueAndStatusHandler - GetQueuePositions - R_LRange failed :: %s', logKey, ex);
                deferred.reject(ex);
            });

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestQueueAndStatusHandler - GetQueuePositions - R_HGet failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestQueueAndStatusHandler - GetQueuePositions failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.SetRequestStatus = setRequestStatus;
module.exports.GetRequestStatus = getRequestStatus;
module.exports.RemoveRequestStatus = removeRequestStatus;

module.exports.SetNextProcessingItem = setNextProcessingItem;
module.exports.AddRequestToQueue = addRequestToQueue;
module.exports.AddRequestToRejectQueue = addRequestToRejectQueue;
module.exports.RemoveRequestFromQueue = removeRequestFromQueue;
module.exports.GetQueuePositions = getQueuePositions;