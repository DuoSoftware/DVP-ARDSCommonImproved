/**
 * Created by Heshan.i on 10/9/2017.
 */

var config = require('config');
var validator = require('validator');
var util = require('util');
var q = require('q');
var restClient = require('../RestClient');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;

var sendResourceStatus = function (logKey, tenant, company, resourceId, additionalParams) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ArdsMonitoringService - SendResourceStatus :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var rUrl = util.format('http://%s/DVP/API/%s/MONITORING/resource/%s/status/publish', config.Services.ardsMonitoringServiceHost, config.Services.ardsMonitoringServiceVersion, resourceId);
        if (validator.isIP(config.Services.ardsMonitoringServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/MONITORING/resource/%s/status/publish', config.Services.ardsMonitoringServiceHost, config.Services.ardsMonitoringServicePort, config.Services.ardsMonitoringServiceVersion, resourceId);
        }

        if(additionalParams){
            rUrl = util.format('%s?%s', rUrl, additionalParams);
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
        logger.error('LogKey: %s - ArdsMonitoringService - SendResourceStatus failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

module.exports.SendResourceStatus = sendResourceStatus;