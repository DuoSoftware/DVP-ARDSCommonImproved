/**
 * Created by Heshan.i on 10/27/2017.
 */

var config = require('config');
var validator = require('validator');
var util = require('util');
var q = require('q');
var restClient = require('../RestClient');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;


var pickResource = function (logKey, tenant, company, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo) {

    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RoutingService - PickResource :: tenant: %d :: company: %d :: resourceCount: %d :: sessionId: %s :: serverType: %s :: requestType: %s :: selectionAlgo: %s :: handlingAlgo: %s :: otherInfo: %s', logKey, tenant, company, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo);

        var rUrl = util.format('http://%s/resourceselection/getresource/%d/%d/%d/%s/%s/%s/%s/%s/%s', config.Services.routingServiceHost, company, tenant, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo);
        if (validator.isIP(config.Services.routingServiceHost)) {
            rUrl = util.format('http://%s:%s/resourceselection/getresource/%d/%d/%d/%s/%s/%s/%s/%s/%s', config.Services.routingServiceHost, config.Services.routingServicePort, company, tenant, resourceCount, sessionId, serverType, requestType, selectionAlgo, handlingAlgo, otherInfo);
        }

        restClient.DoGetInternal(logKey, tenant, company, rUrl).then(function (response) {
            if(response.code === 200){
                deferred.resolve(response.result);
            }else{
                deferred.resolve(null);
            }
        }).catch(function (err) {
            deferred.reject(err);
        });

    } catch (ex) {
        logger.error('LogKey: %s - RoutingService - PickResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.PickResource = pickResource;