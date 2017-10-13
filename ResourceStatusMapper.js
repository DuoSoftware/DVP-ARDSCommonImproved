/**
 * Created by Heshan.i on 10/9/2017.
 */


var deepcopy = require('deepcopy');
var async = require('async');
var q = require('q');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var ardsMonitoringService = require('./services/ArdsMonitoringService');
var resourceService = require('./services/ResourceService');
var scheduleWorkerService = require('./services/ScheduleWorkerService');
var redisHandler = require('./RedisHandler');
var moment = require('moment');
var util = require('util');


var processState = function (logKey, tenant, company, stateKey, resourceId, resourceName, state, reason) {
    var deferred = q.defer();

    logger.info('LogKey: %s - ResourceStatusMapper - processState :: tenant: %d :: company: %d :: resourceId: %s :: resourceName: %s :: state: %s :: reason: %s :: stateKey: %s', logKey, tenant, company, resourceId, resourceName, state, reason, stateKey);

    var date = new Date();
    var statusObj = {ResourceName: resourceName, State: state, Reason: reason, StateChangeTime: date.toISOString()};


    try {
        redisHandler.R_Get(logKey, stateKey).then(function (statusData) {

            if (statusData) {
                var r_StatusObj = JSON.parse(statusData);

                statusObj.Mode = r_StatusObj.Mode;

                if (r_StatusObj && r_StatusObj.State === "NotAvailable" && r_StatusObj.Reason.toLowerCase().indexOf('break') > -1) {

                    var duration = moment(statusObj.StateChangeTime).diff(moment(r_StatusObj.StateChangeTime), 'seconds');
                    resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'ResourceStatus', r_StatusObj.State, r_StatusObj.Reason, '', '', duration).then(function (result) {
                        logger.info('LogKey: %s - ResourceStatusMapper - processState - AddResourceStatusDurationInfo :: Success', logKey);
                    }).catch(function (ex) {
                        logger.info('LogKey: %s - ResourceStatusMapper - processState - AddResourceStatusDurationInfo :: Failed', logKey);
                    });
                }

                if (state === "NotAvailable" && reason === "UnRegister") {

                    statusObj.Mode = "Offline";
                    if (r_StatusObj) {

                        setResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'ResourceStatus', r_StatusObj.State, 'end' + r_StatusObj.Mode, {
                            SessionId: "",
                            Direction: ""
                        }).then(function (result) {
                            logger.info('LogKey: %s - ResourceStatusMapper - processState - setResourceStatusChangeInfo :: Success', logKey);
                        }).catch(function (ex) {
                            logger.info('LogKey: %s - ResourceStatusMapper - processState - setResourceStatusChangeInfo :: Failed', logKey);
                        });

                        if (r_StatusObj.State === "NotAvailable" && r_StatusObj.Reason.toLowerCase().indexOf('break') > -1) {

                            var asyncTasks = [];
                            var duration1 = moment(statusObj.StateChangeTime).diff(moment(statusObjR.StateChangeTime), 'seconds');

                            asyncTasks.push(
                                function (callback) {
                                    setResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'ResourceStatus', 'Available', 'endBreak', {
                                        SessionId: "",
                                        Direction: ""
                                    }).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }
                            );
                            asyncTasks.push(
                                function (callback) {
                                    resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'ResourceStatus', r_StatusObj.State, r_StatusObj.Reason, '', '', duration1).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }
                            );

                            async.parallel(async.reflectAll(asyncTasks), function (err, results) {
                                logger.info('LogKey: %s - ResourceStatusMapper - AddResourceStatusDurationInfo|AddResourceStatusDurationInfo :: Success', logKey);
                            });

                            deferred.resolve(statusObj);

                        } else {

                            deferred.resolve(statusObj);
                        }

                    } else {

                        deferred.resolve(statusObj);
                    }

                } else if ((reason === "Outbound" || reason === "Inbound" || (reason === "Offline" && r_StatusObj.Mode != "Offline")) && r_StatusObj) {

                    statusObj.Mode = reason;

                    setResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'ResourceStatus', r_StatusObj.State, 'end' + r_StatusObj.Mode, {
                        SessionId: "",
                        Direction: ""
                    }).then(function (result) {
                        logger.info('LogKey: %s - ResourceStatusMapper - processState - AddResourceStatusDurationInfo :: Success', logKey);
                    }).catch(function (ex) {
                        logger.info('LogKey: %s - ResourceStatusMapper - processState - AddResourceStatusDurationInfo :: Failed', logKey);
                    });

                    deferred.resolve(statusObj);
                } else {

                    deferred.resolve(statusObj);
                }

            } else {

                statusObj.Mode = 'Offline';
                deferred.resolve(statusObj);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceStatusMapper - processState failed :: %s', logKey, ex);
            statusObj.Mode = 'Offline';
            deferred.resolve(statusObj);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceStatusMapper - processState failed :: %s', logKey, ex);
        statusObj.Mode = 'Offline';
        deferred.resolve(statusObj);
    }

    return deferred.promise;
};

var validateState = function (logKey, tenant, company, resourceId, reason) {
    var deferred = q.defer();

    var returnData = {
        isRequestValid: false,
        message: ""
    };

    try{
        var reasonValue = reason.toLowerCase();
        if (reasonValue === "register" || reasonValue === "offline" || reasonValue === "unregister") {

            returnData.isRequestValid = true;
            deferred.resolve(returnData);
        }else{

            var resourceKey = util.format('Resource:%d:%d:%s', tenant, company, resourceId);
            redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

                if (resourceData) {

                    var resourceObj = JSON.parse(resourceData);
                    if (resourceObj.LoginTasks && (resourceObj.LoginTasks.length === 0 || resourceObj.LoginTasks.indexOf("CALL") === -1)) {

                        returnData.isRequestValid = true;
                        returnData.message = "No login task found, proceed to state change";
                        deferred.resolve(returnData);

                    } else {
                        if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length > 0) {

                            var regexPattern = /CSlotInfo:([0-9]*:)*CALL:[0-9]*/g;
                            var callSlots = resourceObj.ConcurrencyInfo.filter(function (cInfo) {
                                return regexPattern.test(cInfo);
                            });

                            if (callSlots && callSlots.length > 0) {

                                redisHandler.R_MGet(logKey, callSlots).then(function (slotsData) {

                                    if(slotsData){

                                        var slotAvailability = true;

                                        for (var i = 0; i < slotsData.length; i++) {

                                            var slotObj = JSON.parse(slotsData[i]);
                                            slotAvailability = slotAvailability && (slotObj.State.toLowerCase() !== "connected" && !slotObj.FreezeAfterWorkTime);

                                            if(slotObj.State.toLowerCase() === "reserved"){
                                                var reservedTimeDiff = moment().diff(moment(slotObj.StateChangeTime), 'seconds');
                                                slotAvailability = slotAvailability && (reservedTimeDiff > slotObj.MaxReservedTime);
                                            }

                                            if (!slotAvailability)
                                                break;

                                        }

                                        if(slotAvailability){

                                            returnData.isRequestValid = true;
                                            returnData.message = "All slots are free to accept the request";
                                            deferred.resolve(returnData);

                                        }else{

                                            returnData.isRequestValid = false;
                                            returnData.message = "Can't accept the request while on call";
                                            deferred.resolve(returnData);

                                        }

                                    }else{
                                        returnData.isRequestValid = false;
                                        returnData.message = "Error occurred in processing state change request";
                                        deferred.resolve(returnData);
                                    }
                                    
                                }).catch(function (ex) {

                                    returnData.isRequestValid = false;
                                    returnData.message = "Error occurred in processing state change request";
                                    deferred.resolve(returnData);
                                });

                            }else{

                                returnData.isRequestValid = true;
                                returnData.message = "No call slot found, proceed to state change";
                                deferred.resolve(returnData);
                            }

                        }else{

                            returnData.isRequestValid = true;
                            returnData.message = "No concurrency info  found, proceed to state change";
                            deferred.resolve(returnData);
                        }
                    }

                }else{
                    returnData.isRequestValid = false;
                    returnData.message = "Error occurred in processing state change request";
                    deferred.resolve(returnData);
                }

            }).catch(function (ex) {

                returnData.isRequestValid = false;
                returnData.message = "Error occurred in processing state change request";
                deferred.resolve(returnData);
            });
        }
    }catch(ex){
        logger.error('LogKey: %s - ResourceStatusMapper - validateState failed :: %s', logKey, ex);

        returnData.isRequestValid = false;
        returnData.message = "Error occurred in processing state change request";
        deferred.resolve(returnData);
    }

    return deferred.promise;
};


var setResourceStatusChangeInfo = function (logKey, tenant, company, resourceId, statusType, status, reason, otherData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceStatusMapper - SetResourceStatusChangeInfo :: tenant: %d :: company: %d :: resourceId: %s :: statusType: %s :: status: %s :: reason: %s :: otherData: %j', logKey, tenant, company, resourceId, statusType, status, reason, otherData);

        var param2 = deepcopy(reason);
        var dashBoardReason = deepcopy(reason);
        var sessionData = otherData.SessionId ? otherData.SessionId : "";

        if ((status.toLowerCase() === "connected" || (status.toLowerCase() === 'completed' && reason.toLowerCase() === 'afterwork')) && otherData) {
            param2 = util.format('%s%s', param2, otherData.Direction);
        }

        if (reason && reason.toLowerCase() !== "endbreak" && reason.toLowerCase().indexOf('break') > -1) {
            dashBoardReason = 'Break';
        }

        var pubMessage = util.format("EVENT:%d:%d:%s:%s:%s:%s:%s:%s:YYYY", tenant, company, statusType, status, dashBoardReason, resourceId, param2, resourceId);

        var asyncTasks = [
            function (callback) {
                redisHandler.R_Publish(logKey, "events", pubMessage).then(function (result) {
                    callback(null, result);
                }).catch(function (ex) {
                    callback(ex, null);
                });
            },
            function (callback) {
                ardsMonitoringService.SendResourceStatus(logKey, tenant, company, resourceId, undefined).then(function (result) {
                    callback(null, result);
                }).catch(function (ex) {
                    callback(ex, null);
                });
            },
            function (callback) {
                resourceService.AddResourceStatusChangeInfo(logKey, tenant, company, resourceId, statusType, status, reason, sessionData).then(function (result) {
                    callback(null, result);
                }).catch(function (ex) {
                    callback(ex, null);
                });
            }
        ];

        async.parallel(async.reflectAll(asyncTasks), function () {
            logger.info('LogKey: %s - ResourceStatusMapper - SetResourceStatusChangeInfo :: Process finished', logKey);
            deferred.resolve("Set resource status change info process finished");
        });


    } catch (ex) {
        logger.error('LogKey: %s - ResourceStatusMapper - SetResourceStatusChangeInfo failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var setResourceState = function (logKey, tenant, company, resourceId, resourceName, state, reason) {
    var deferred = q.defer();

    try{

        validateState(logKey, tenant, company, resourceId, reason).then(function (result) {

            if(result.isRequestValid){

                var stateKey = util.format('ResourceState:%d:%d:%s', tenant, company, resourceId);
                processState(logKey, tenant, company, stateKey, resourceId, resourceName, state, reason).then(function (statusObj) {

                    redisHandler.R_Set(logKey, stateKey, JSON.stringify(statusObj)).then(function () {

                        setResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'ResourceStatus', state, reason, {
                            SessionId: "",
                            Direction: ""
                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceStatusMapper - SetResourceState :: Success', logKey);
                            deferred.resolve(result);
                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceStatusMapper - SetResourceState :: Failed :: %s', logKey, ex);
                            deferred.resolve('Set resource state failed');
                        });

                        if (reason && reason.toLowerCase() !== "endbreak" && reason.toLowerCase().indexOf('break') > -1) {
                            scheduleWorkerService.startBreak(company, tenant, resourceName, resourceId, reason, logKey);
                        }

                        if (reason && reason.toLowerCase() !== "break" && reason.toLowerCase().indexOf('endbreak') > -1) {
                            scheduleWorkerService.endBreak(company, tenant, resourceName, logKey);
                        }

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceStatusMapper - SetResourceState :: Failed :: %s', logKey, ex);
                        deferred.resolve('Set resource state failed');
                    });
                });
            }else{

                logger.error('LogKey: %s - ResourceStatusMapper - SetResourceState :: Failed :: %s', logKey, ex);
                deferred.resolve(result.message);
            }
        });

    }catch(ex){

        logger.error('LogKey: %s - ResourceStatusMapper - SetResourceState failed :: %s', logKey, ex);
        deferred.resolve('Set resource state failed');
    }

    return deferred.promise;
};


module.exports.SetResourceStatusChangeInfo = setResourceStatusChangeInfo;
module.exports.SetResourceState = setResourceState;