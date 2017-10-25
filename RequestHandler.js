/**
 * Created by Heshan.i on 10/25/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var redisHandler = require('./RedisHandler');
var tagHandler = require('./TagHandler');
var requestServerHandler = require('./RequestServerHandler');
var requestMetadataHandler = require('./RequestMetaDataHandler');
var requestQueueAndStatusHandler = require('./RequestQueueAndStatusHandler');
var q = require('q');
var async = require('async');
var util = require('util');
var config = require('config');

var sortString = function (a, b) {
    a = a.toLowerCase();
    b = b.toLowerCase();
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
};

var preProcessRequestData = function (logKey, tenant, company, preRequestData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestHandler - PreProcessRequestData :: tenant: %d :: company: %d :: preRequestData: %j', logKey, tenant, company, preRequestData);

        if (preRequestData.RequestServerId && preRequestData.SessionId && preRequestData.Attributes && preRequestData.Attributes.length > 0) {

            var date = new Date();
            var requestObj = {
                Company: company,
                Tenant: tenant,
                SessionId: preRequestData.SessionId,
                ArriveTime: date.toISOString(),
                Priority: (preRequestData.Priority) ? preRequestData.Priority : '0',
                ResourceCount: (preRequestData.ResourceCount) ? preRequestData.ResourceCount : 1,
                OtherInfo: preRequestData.OtherInfo,
                LbIp: config.Host.LBIP,
                LbPort: config.Host.LBPort
            };

            requestServerHandler.GetRequestServer(logKey, tenant, company, preRequestData.RequestServerId).then(function (requestServerData) {

                requestObj.RequestServerId = requestServerData.ServerID;
                requestObj.ServerType = requestServerData.ServerType;
                requestObj.RequestType = requestServerData.RequestType;
                requestObj.RequestServerUrl = requestServerData.CallbackUrl;
                requestObj.CallbackOption = requestServerData.CallbackOption;
                requestObj.QPositionUrl = requestServerData.QueuePositionCallbackUrl;

                return requestMetadataHandler.GetRequestMetaData(logKey, tenant, company, requestServerData.ServerType, requestServerData.RequestType);

            }).then(function (requestMetadata) {

                if (requestMetadata && requestMetadata.AttributeMeta && requestMetadata.AttributeMeta.length > 0) {

                    if (requestMetadata.ServingAlgo && requestMetadata.HandlingAlgo && requestMetadata.SelectionAlgo && requestMetadata.ReqHandlingAlgo) {

                        var attributeDataList = [];
                        var requestAttributes = [];
                        var requestAttributeNames = [];

                        preRequestData.Attributes.forEach(function (requestAttributeId) {

                            requestMetadata.AttributeMeta.forEach(function (attributeMetadata) {

                                if (attributeMetadata && attributeMetadata.HandlingType === requestObj.RequestType && attributeMetadata.AttributeCode.indexOf(requestAttributeId) > -1) {

                                    var processedAttributeData = attributeDataList.filter(function (attributeData) {
                                        return attributeData.AttributeGroupName === attributeMetadata.AttributeGroupName && attributeData.HandlingType === attributeMetadata.HandlingType;
                                    });
                                    var attributeDetail = attributeMetadata.AttributeDetails.filter(function (attributeDetail) {
                                        if (attributeDetail.Id === requestAttributeId)
                                            return attributeDetail;
                                    });

                                    if (processedAttributeData && processedAttributeData.length > 0) {
                                        processedAttributeData[0].AttributeCode.push(requestAttributeId);
                                        requestAttributes.push(requestAttributeId);
                                        if (attributeDetail && attributeDetail.length > 0) {
                                            processedAttributeData[0].AttributeNames.push(attributeDetail[0].Name);
                                            requestAttributeNames.push(attributeDetail[0].Name);
                                        }
                                    } else {
                                        attributeDataList.push(
                                            {
                                                AttributeGroupName: attributeMetadata.AttributeGroupName,
                                                HandlingType: attributeMetadata.HandlingType,
                                                AttributeCode: [
                                                    requestAttributeId
                                                ],
                                                WeightPercentage: attributeMetadata.WeightPercentage,
                                                AttributeNames: (attributeDetail && attributeDetail.length > 0) ? [attributeDetail[0].Name] : []
                                            }
                                        );

                                        requestAttributes.push(requestAttributeId);
                                        if (attributeDetail && attributeDetail.length > 0)
                                            requestAttributeNames.push(attributeDetail[0].Name);
                                    }
                                }

                            });

                        });

                        if (attributeDataList.length > 0) {

                            requestObj.ServingAlgo = requestMetadata.ServingAlgo;
                            requestObj.HandlingAlgo = requestMetadata.HandlingAlgo;
                            requestObj.SelectionAlgo = requestMetadata.SelectionAlgo;
                            requestObj.ReqHandlingAlgo = requestMetadata.ReqHandlingAlgo;
                            requestObj.ReqSelectionAlgo = (requestMetadata.ReqSelectionAlgo) ? requestMetadata.ReqSelectionAlgo : 'LONGESTWAITING';
                            requestObj.AttributeInfo = attributeDataList;

                            var sortedRequestAttributes = requestAttributes.sort(sortString);
                            var attributeDataString = util.format('attribute_%s', sortedRequestAttributes.join(":attribute_"));
                            var queueId = util.format('Queue:%d:%d:%s:%s:%s:%s', requestObj.Company, requestObj.Tenant, requestObj.ServerType, requestObj.RequestType, attributeDataString, requestObj.Priority);
                            var queueSettingId = util.format('Queue:%d:%d:%s:%s:%s', requestObj.Company, requestObj.Tenant, requestObj.ServerType, requestObj.RequestType, attributeDataString);

                            requestObj.QueueId = queueId;

                            resourceService.GetQueueSetting(logKey, tenant, company, queueSettingId).then(function (queueSettingData) {

                                if (queueSettingData && queueSettingData.IsSuccess && queueSettingData.Result) {

                                    var queueSetting = queueSettingData.Result;
                                    requestObj.QPositionEnable = queueSetting.PublishPosition ? queueSetting.PublishPosition : false;
                                    requestObj.QueueName = queueSetting.QueueName ? queueSetting.QueueName : util.format('%s', requestAttributeNames.join("-"));

                                } else {

                                    requestObj.QPositionEnable = false;
                                    requestObj.QueueName = util.format('%s', requestAttributeNames.join("-"));

                                    resourceService.AddQueueSetting(logKey, tenant, company, requestObj.QueueName, sortedRequestAttributes, requestObj.ServerType, requestObj.RequestType);

                                }

                                logger.info('LogKey: %s - RequestHandler - PreProcessRequestData :: success', logKey);
                                deferred.resolve({Request: requestObj, Attributes: sortedRequestAttributes});

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - RequestHandler - PreProcessRequestData - GetQueueSetting failed :: %s', logKey, ex);

                                requestObj.QPositionEnable = false;
                                requestObj.QueueName = util.format('%s', requestAttributeNames.join("-"));

                                deferred.resolve({Request: requestObj, Attributes: sortedRequestAttributes});

                            });

                        } else {

                            logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Invalid request attributes', logKey);
                            deferred.reject('Invalid request attributes');

                        }

                    } else {

                        logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Invalid request metadata', logKey);
                        deferred.reject('Invalid request metadata');
                    }

                } else {

                    logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: No registered attribute group found in request metadata', logKey);
                    deferred.reject('No registered attribute group found in request metadata');
                }

            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestHandler - PreProcessRequestData - GetRequestServer failed :: %s', logKey, ex);
                deferred.reject(ex);
            });

        } else {

            logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: Insufficient data to process', logKey);
            deferred.reject('Insufficient data to process');
        }

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - PreProcessRequestData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


var addRequest = function (logKey, tenant, company, preRequestData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestHandler - AddRequest :: tenant: %d :: company: %d :: preRequestData: %j', logKey, tenant, company, preRequestData);

        preProcessRequestData(logKey, tenant, company, preRequestData).then(function (preProcessResponse) {

            var requestObj = preProcessResponse.Request;
            var requestKey = util.format('Request:%d:%d:%s', requestObj.Tenant, requestObj.Company, requestObj.SessionId);
            var requestTags = [
                "company_" + requestObj.Company,
                "tenant_" + requestObj.Tenant,
                "sessionId_" + requestObj.SessionId,
                "reqServerId_" + requestObj.RequestServerId,
                "priority_" + requestObj.Priority,
                "objType_Request"
            ];

            preProcessResponse.Attributes.forEach(function (attribute) {
                requestTags.push(util.format('%s:attribute_%s', requestObj.RequestType, attribute));
            });

            tagHandler.SetTags(logKey, 'Tag:Request', requestTags, requestKey).then(function (requestTagResult) {

                logger.info('LogKey: %s - RequestHandler - AddRequest - SetTags success :: %s', logKey, requestTagResult);
                return redisHandler.R_Set(logKey, requestKey, JSON.stringify(requestObj));

            }).then(function (requestResult) {

                logger.info('LogKey: %s - RequestHandler - AddRequest - R_Set request success :: %s', logKey, requestResult);

                switch (requestObj.ReqHandlingAlgo.toLowerCase()) {
                    case 'queue':

                        requestQueueAndStatusHandler.AddRequestToQueue(logKey, requestObj.Tenant, requestObj.Company, requestObj.RequestType, requestObj.QueueId, requestObj.SessionId).then(function (queuePosition) {

                            logger.error('LogKey: %s - RequestHandler - AddRequest - AddRequestToQueue success :: %s', logKey, queuePosition);
                            deferred.resolve({
                                Position: queuePosition,
                                QueueName: requestObj.QueueName,
                                Message: "Request added to queue. sessionId :: " + requestObj.SessionId
                            });

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestHandler - AddRequest - AddRequestToQueue failed :: %s', logKey, ex);
                            //Todo remove request
                            deferred.reject('Add Request to Queue Failed. sessionId :: " + requestObj.SessionId');
                        });
                        break;

                    case 'direct':
                        break;
                    default :

                        logger.error('LogKey: %s - RequestHandler - AddRequest - No request handling algorithm found', logKey);
                        //Todo remove request
                        deferred.reject('No request handling algorithm found');
                        break;

                }
            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestHandler - AddRequest - set request data failed :: %s', logKey, ex);
                //Todo remove request
                deferred.reject('Add request failed :: Set request data');
            });

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestHandler - AddRequest - preProcessRequestData failed :: %s', logKey, ex);
            deferred.reject(ex);
        })

    } catch (ex) {

        logger.error('LogKey: %s - RequestHandler - AddRequest failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};