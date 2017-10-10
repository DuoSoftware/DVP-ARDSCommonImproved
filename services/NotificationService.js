/**
 * Created by Heshan.i on 10/9/2017.
 */

var config = require('config');
var validator = require('validator');
var util = require('util');
var q = require('q');
var restClient = require('../RestClient');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;


var sendNotificationToRoom = function (logKey, tenant, company, roomName, eventName, msgData) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - NotificationService - SendNotificationToRoom :: tenant: %d :: company: %d :: roomName: %s :: eventName: %s :: msgData: %j', logKey, tenant, company, roomName, eventName, msgData);

        var rUrl = util.format('http://%s/DVP/API/%s/NotificationService/Notification/initiate/%s', config.Services.notificationServiceHost, config.Services.notificationServiceVersion, roomName);
        if (validator.isIP(config.Services.notificationServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/NotificationService/Notification/initiate/%s', config.Services.notificationServiceHost, config.Services.notificationServicePort, config.Services.notificationServiceVersion, roomName);
        }

        restClient.DoPostNotification(logKey, tenant, company, rUrl, msgData).then(function (response) {
            if(response.statusCode === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - NotificationService - SendNotificationToRoom failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var sendNotificationInitiate = function (logKey, tenant, company, eventName, eventUuid, payload) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - NotificationService - SendNotificationInitiate :: tenant: %d :: company: %d :: eventName: %s :: eventUuid: %s :: payload: %j', logKey, tenant, company, eventName, eventUuid, payload);

        var rUrl = util.format('http://%s/DVP/API/%s/NotificationService/Notification/initiate', config.Services.notificationServiceHost, config.Services.notificationServiceVersion);
        if (validator.isIP(config.Services.notificationServiceHost)) {
            rUrl = util.format('http://%s:%s/DVP/API/%s/NotificationService/Notification/initiate', config.Services.notificationServiceHost, config.Services.notificationServicePort, config.Services.notificationServiceVersion);
        }

        restClient.DoPostNotification(logKey, tenant, company, rUrl, payload).then(function (response) {
            if(response.statusCode >= 200 && response.statusCode <= 299){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(undefined);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - NotificationService - SendNotificationInitiate failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.SendNotificationToRoom = sendNotificationToRoom;
module.exports.SendNotificationInitiate = sendNotificationInitiate;