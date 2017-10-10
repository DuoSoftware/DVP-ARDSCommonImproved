/**
 * Created by Heshan.i on 10/6/2017.
 */

var config = require('config');
var validator = require('validator');
var util = require('util');
var q = require('q');
var restClient = require('../RestClient');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;


//------------------Resource And Attributes-----------------------------------------

var getResourceDetails = function (logKey, tenant, company, resourceId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetResourceDetails :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Resource/%s', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetResourceDetails failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getAttributeGroupWithDetails = function (logKey, tenant, company, attributeGroupId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetAttributeGroupWithDetails :: tenant: %d :: company: %d :: attributeGroupId: %s', logKey, tenant, company, attributeGroupId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Group/%d/Attribute/Details', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, attributeGroupId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Group/%d/Attribute/Details', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, attributeGroupId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetAttributeGroupWithDetails failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getResourceTaskDetails = function (logKey, tenant, company, resourceId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetResourceTaskDetails :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Resource/%s/Tasks', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/Tasks', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetResourceTaskDetails failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getResourceAttributeDetails = function (logKey, tenant, company, resourceTaskId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetResourceAttributeDetails :: tenant: %d :: company: %d :: resourceTaskId: %s', logKey, tenant, company, resourceTaskId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/ResourceTask/%d/Attributes', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceTaskId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/ResourceTask/%d/Attributes', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceTaskId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetResourceAttributeDetails failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getAttribute = function (logKey, tenant, company, attributeId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetAttribute :: tenant: %d :: company: %d :: attributeId: %s', logKey, tenant, company, attributeId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Attribute/%s', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, attributeId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Attribute/%s', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, attributeId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetAttribute failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


//------------------Resource Status And Durations-----------------------------------

var addResourceStatusChangeInfo = function (logKey, tenant, company, resourceId, statusType, status, reason, sessionId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - AddResourceStatusChangeInfo :: tenant: %d :: company: %d :: resourceId: %s :: statusType: %s :: status: %s :: reason: %s :: sessionId: %s', logKey, tenant, company, resourceId, statusType, status, reason, sessionId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Resource/%s/Status', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/Status', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        var statusObj = {StatusType:statusType, Status:status, Reason:reason, OtherData: sessionId};

        restClient.DoPostInternal(logKey, tenant, company, rUrl, statusObj).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - AddResourceStatusChangeInfo failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var addResourceStatusDurationInfo = function (logKey, tenant, company, resourceId, statusType, status, reason, otherData, sessionId, duration) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - AddResourceStatusDurationInfo :: tenant: %d :: company: %d :: resourceId: %s :: statusType: %s :: status: %s :: reason: %s :: otherData: %s :: sessionId: %s :: duration: %d', logKey, tenant, company, resourceId, statusType, status, reason, otherData, sessionId, duration);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Resource/%s/StatusDuration', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/StatusDuration', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        var durationObj = {StatusType:statusType, Status:status, Reason:reason, OtherData: otherData, SessionId: sessionId, Duration: duration};

        restClient.DoPostInternal(logKey, tenant, company, rUrl, durationObj).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - AddResourceStatusDurationInfo failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var addResourceTaskRejectInfo = function (logKey, tenant, company, resourceId, task, reason, otherData, sessionId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - AddResourceTaskRejectInfo :: tenant: %d :: company: %d :: resourceId: %s :: task: %s :: reason: %s :: otherData: %s :: sessionId: %s', logKey, tenant, company, resourceId, task, reason, otherData, sessionId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/Resource/%s/TaskRejectInfo', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/Resource/%s/TaskRejectInfo', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        var rejectObj = {Task:task, Reason:reason, OtherData: otherData, SessionId: sessionId};

        restClient.DoPostInternal(logKey, tenant, company, rUrl, rejectObj).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - AddResourceTaskRejectInfo failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


//------------------Queue Settings--------------------------------------------------

var getQueueSetting = function (logKey, tenant, company, queueId) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - GetQueueSetting :: tenant: %d :: company: %d :: queueId: %s', logKey, tenant, company, queueId);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/QueueSetting/%s', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, queueId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/QueueSetting/%s', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, queueId);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - GetQueueSetting failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var addQueueSetting = function (logKey, tenant, company, queueName, skills, serverType, requestType) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceService - AddQueueSetting :: tenant: %d :: company: %d :: queueName: %s :: skills: %j :: serverType: %s :: requestType: %s', logKey, tenant, company, queueName, skills, serverType, requestType);

        var rUrl = util.format('http://%s/DVP/API/%s/ResourceManager/QueueSetting', config.Services.resourceServiceHost, config.Services.resourceServiceVersion, resourceId);
        if (validator.isIP(config.Services.resourceServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/ResourceManager/QueueSetting', config.Services.resourceServiceHost, config.Services.resourceServicePort, config.Services.resourceServiceVersion, resourceId);
        }

        var queueSettingObj = {QueueName :queueName,MaxWaitTime:"0",Skills:skills,PublishPosition:"false",CallAbandonedThreshold:"0",ServerType:serverType,RequestType:requestType};

        restClient.DoPostInternal(logKey, tenant, company, rUrl, queueSettingObj).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceService - AddQueueSetting failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.GetResourceDetails = getResourceDetails;
module.exports.GetAttributeGroupWithDetails = getAttributeGroupWithDetails;
module.exports.GetResourceTaskDetails = getResourceTaskDetails;
module.exports.GetResourceAttributeDetails = getResourceAttributeDetails;
module.exports.GetAttribute = getAttribute;

module.exports.AddResourceStatusChangeInfo = addResourceStatusChangeInfo;
module.exports.AddResourceStatusDurationInfo = addResourceStatusDurationInfo;
module.exports.AddResourceTaskRejectInfo = addResourceTaskRejectInfo;

module.exports.GetQueueSetting = getQueueSetting;
module.exports.AddQueueSetting = addQueueSetting;