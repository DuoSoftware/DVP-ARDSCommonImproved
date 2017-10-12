/**
 * Created by Heshan.i on 10/6/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var q = require('q');
var request = require('request');
var config = require('config');
var util = require('util');


var doGetInternal = function (logKey, tenant, company, url) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Request HTTP GET Internal :: tenant: %d :: company: %d :: url: %s', logKey, tenant, company, url);

        var options = {
            url: url,
            method: 'GET',
            headers: {
                'content-type': 'application/json',
                'authorization': util.format('bearer %s', config.Services.accessToken),
                'companyinfo': util.format('%d:%d', tenant, company)
            }
        };

        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                logger.error('LogKey: %s - Request HTTP GET Internal failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                var response = {code: httpResponse.statusCode, result: body?JSON.parse(body):undefined};
                logger.info('LogKey: %s - Request HTTP GET Internal success :: %j', logKey, response);
                deferred.resolve(response);
            }
        });

    }catch(ex){
        logger.error('LogKey: %s - Request HTTP GET Internal Internal failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var doPostInternal = function (logKey, tenant, company, url, postData) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Request HTTP POST Internal :: tenant: %d :: company: %d :: url: %s :: url: %j', logKey, tenant, company, url, postData);

        var options = {
            url: url,
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'authorization': util.format('bearer %s', config.Services.accessToken),
                'companyinfo': util.format('%d:%d', tenant, company)
            },
            body: postData? JSON.stringify(postData): ""
        };

        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                logger.error('LogKey: %s - Request HTTP POST Internal failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                var response = {code: httpResponse.statusCode, result: body?JSON.parse(body):undefined};
                logger.info('LogKey: %s - Request HTTP POST Internal success :: %j', logKey, response);
                deferred.resolve(response);
            }
        });

    }catch(ex){
        logger.error('LogKey: %s - Request HTTP POST Internal failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var doPostNotification = function (logKey, tenant, company, url, eventName, eventUuid, postData) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Request HTTP POST Notification :: tenant: %d :: company: %d :: url: %s :: eventName: %s :: eventUuid: %s :: postData: %j', logKey, tenant, company, url, eventName, eventUuid, postData);

        var options = {
            url: url,
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'authorization': util.format('bearer %s', config.Services.accessToken),
                'companyinfo': util.format('%d:%d', tenant, company),
                "eventname": eventName,
                "eventuuid": eventUuid
            },
            body: postData? JSON.stringify(postData): ""
        };

        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                logger.error('LogKey: %s - Request HTTP POST Notification failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                var response = {code: httpResponse.statusCode, result: body?JSON.parse(body):undefined};
                logger.info('LogKey: %s - Request HTTP POST Notification success :: %j', logKey, response);
                deferred.resolve(response);
            }
        });

    }catch(ex){
        logger.error('LogKey: %s - Request HTTP POST Notification failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var doGetExternal = function (logKey, url) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Request HTTP GET External :: url: %s', logKey, url);

        var options = {
            url: url,
            method: 'GET',
            headers: {
                'content-type': 'text/plain'
            }
        };

        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                logger.error('LogKey: %s - Request HTTP GET External failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                var response = {code: httpResponse.statusCode, result: body};
                logger.info('LogKey: %s - Request HTTP GET External success :: %j', logKey, response);
                deferred.resolve(response);
            }
        });

    }catch(ex){
        logger.error('LogKey: %s - Request HTTP GET External failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var doPostExternal = function (logKey, url, postData) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - Request HTTP POST External :: url: %s', logKey, url);

        var options = {
            url: url,
            method: 'POST',
            headers: {
                'content-type': 'application/json'
            },
            body: JSON.stringify(postData)
        };

        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                logger.error('LogKey: %s - Request HTTP POST External failed :: %s', logKey, err);
                deferred.reject(err);
            }else{
                var response = {code: httpResponse.statusCode, result: body?JSON.parse(body):undefined};
                logger.info('LogKey: %s - Request HTTP POST External success :: %j', logKey, response);
                deferred.resolve(response);
            }
        });

    }catch(ex){
        logger.error('LogKey: %s - Request HTTP POST External failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

module.exports.DoGetInternal = doGetInternal;
module.exports.DoPostInternal = doPostInternal;
module.exports.DoPostNotification = doPostNotification;
module.exports.DoGetExternal = doGetExternal;
module.exports.DoPostExternal = doPostExternal;