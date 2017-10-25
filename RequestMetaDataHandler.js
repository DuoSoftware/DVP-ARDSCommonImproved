/**
 * Created by Heshan.i on 10/20/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var redisHandler = require('./RedisHandler');
var q = require('q');
var async = require('async');
var util = require('util');
var dbConn = require('dvp-dbmodels');


var getAttributeGroupData = function (logKey, tenant, company, attributeGroupIds) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData :: tenant: %d :: company: %d :: attributeGroupIds: %j', logKey, tenant, company, attributeGroupIds);

        var asyncTasks = [];
        attributeGroupIds.forEach(function (attributeGroupId) {
            asyncTasks.push(
                function (callback) {
                    resourceService.GetAttributeGroupWithDetails(logKey, tenant, company, attributeGroupId).then(function (result) {

                        if (result) {

                            if (result.IsSuccess) {

                                var attributeGroupWithDetails = result.Result;
                                if (attributeGroupWithDetails) {

                                    var attributeIdList = [];
                                    var attributeDetailList = [];
                                    attributeGroupWithDetails.ResAttributeGroups.forEach(function (attributeData) {

                                        if (attributeData && attributeData.AttributeId) {
                                            attributeIdList.push(attributeData.AttributeId.toString());

                                            if (attributeData.ResAttribute && attributeData.ResAttribute.Attribute) {
                                                attributeDetailList.push(
                                                    {
                                                        Id: attributeData.AttributeId.toString(),
                                                        Name: attributeData.ResAttribute.Attribute
                                                    }
                                                );
                                            }
                                        }

                                    });

                                    var attributeGroupData = {
                                        GroupId: attributeGroupWithDetails.GroupId,
                                        AttributeGroupName: attributeGroupWithDetails.GroupName,
                                        HandlingType: attributeGroupWithDetails.GroupType,
                                        WeightPercentage: attributeGroupWithDetails.Percentage.toString(),
                                        AttributeCode: attributeIdList,
                                        AttributeDetails: attributeDetailList
                                    };

                                    callback(null, attributeGroupData);

                                } else {

                                    logger.error('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData - GetAttributeGroupWithDetails :: Service returns empty data', logKey);
                                    callback(new Error('Service returns empty data'), null);
                                }

                            } else {

                                logger.error('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData - GetAttributeGroupWithDetails :: %s', logKey, result.Message);
                                callback(new Error(result.Message), null);
                            }

                        } else {

                            logger.error('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData - GetAttributeGroupWithDetails :: Invalid response received from resource service', logKey);
                            callback(new Error('Invalid response received from resource service'), null);
                        }

                    }).catch(function (ex) {

                        callback(ex, null);
                    });
                }
            );
        });

        async.parallel(async.reflectAll(asyncTasks), function (err, results) {

            if (err) {

                logger.error('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData failed :: %s', logKey, err);
                deferred.reject(err);
            } else {

                var attributeGroupData = [];
                results.forEach(function (result) {

                    if (result && !result.error && result.value) {
                        attributeGroupData.push(result.value);
                    }

                });

                deferred.resolve(attributeGroupData);
            }

        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestMetaDataHandler - GetAttributeGroupData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


var addRequestMetaData = function (logKey, tenant, company, metaData) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData :: tenant: %d :: company: %d :: metaData: %j', logKey, tenant, company, metaData);

        if (metaData && metaData.ServerType && metaData.RequestType && metaData.AttributeGroups) {

            var metaDataKey = util.format('ReqMETA:%d:%d:%s:%s', tenant, company, metaData.ServerType, metaData.RequestType);


            getAttributeGroupData(logKey, tenant, company, metaData.AttributeGroups).then(function (attributeGroupData) {

                metaData.Tenant = tenant;
                metaData.Company = tenant;
                metaData.AttributeMeta = attributeGroupData;
                metaData.AttributeGroups = attributeGroupData.map(function (attributeGroup) {
                    return attributeGroup.GroupId;
                });

                redisHandler.R_Exists(logKey, metaDataKey).then(function (keyExist) {

                    if (keyExist === 0) {

                        dbConn.ArdsRequestMetaData.create(
                            {
                                Tenant: metaData.Tenant,
                                Company: metaData.Company,
                                ServerType: metaData.ServerType,
                                RequestType: metaData.RequestType,
                                AttributeGroups: JSON.stringify(metaData.AttributeGroups),
                                ServingAlgo: metaData.ServingAlgo,
                                HandlingAlgo: metaData.HandlingAlgo,
                                SelectionAlgo: metaData.SelectionAlgo,
                                ReqHandlingAlgo: metaData.ReqHandlingAlgo,
                                ReqSelectionAlgo: metaData.ReqSelectionAlgo,
                                MaxReservedTime: metaData.MaxReservedTime,
                                MaxRejectCount: metaData.MaxRejectCount,
                                MaxAfterWorkTime: metaData.MaxAfterWorkTime,
                                MaxFreezeTime: metaData.MaxFreezeTime
                            }
                        ).then(function () {

                                redisHandler.R_Set(logKey, metaDataKey, JSON.stringify(metaData));
                                deferred.resolve('Add request metadata success');

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - dbConn.ArdsRequestMetaData failed :: %s', logKey, ex);
                                if (ex.name == "SequelizeUniqueConstraintError") {

                                    deferred.reject('Request metadata already exists');
                                } else {

                                    deferred.reject(ex);
                                }
                            });


                    } else {

                        dbConn.ArdsRequestMetaData.find({where: [{Tenant: metaData.Tenant}, {Company: metaData.Company}, {ServerType: metaData.ServerType}, {RequestType: metaData.RequestType}]}).then(function (results) {
                            if (results) {
                                results.updateAttributes({
                                    AttributeGroups: JSON.stringify(metaData.AttributeGroups),
                                    ServingAlgo: metaData.ServingAlgo,
                                    HandlingAlgo: metaData.HandlingAlgo,
                                    SelectionAlgo: metaData.SelectionAlgo,
                                    ReqHandlingAlgo: metaData.ReqHandlingAlgo,
                                    ReqSelectionAlgo: metaData.ReqSelectionAlgo,
                                    MaxReservedTime: metaData.MaxReservedTime,
                                    MaxRejectCount: metaData.MaxRejectCount,
                                    MaxAfterWorkTime: metaData.MaxAfterWorkTime,
                                    MaxFreezeTime: metaData.MaxFreezeTime
                                }).then(function () {
                                    redisHandler.R_Set(logKey, metaDataKey, JSON.stringify(metaData));
                                    deferred.resolve('Update request metadata success');
                                }).error(function (err) {
                                    logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - update existing data record failed :: %s', logKey, err);
                                    deferred.reject('Update existing data record failed');
                                });
                            } else {

                                logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - No existing data record found', logKey);
                                deferred.reject('No existing data record found');
                            }
                        }).error(function (err) {

                            logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - find existing data record failed :: %s', logKey, err);
                            deferred.reject('Find existing data record failed');
                        });

                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - getAttributeGroupData failed :: %s', logKey, ex);
                    deferred.reject(ex);
                });

            }).catch(function (ex) {

                logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - R_Exists failed :: %s', logKey, ex);
                deferred.reject(ex);
            });

        } else {

            logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData - Insufficient data to proceed', logKey);
            deferred.reject('Insufficient data to proceed');
        }

    } catch (ex) {

        logger.error('LogKey: %s - RequestMetaDataHandler - AddRequestMetaData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getRequestMetaData = function (logKey, tenant, company, serverType, requestType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestMetaDataHandler - GetRequestMetaData :: tenant: %d :: company: %d :: serverType: %s :: requestType: %s', logKey, tenant, company, serverType, requestType);

        var metaDataKey = util.format('ReqMETA:%d:%d:%s:%s', tenant, company, serverType, requestType);

        redisHandler.R_Get(logKey, metaDataKey).then(function (metaData) {

            if (metaData) {

                deferred.resolve(JSON.parse(metaData));

            } else {

                dbConn.ArdsRequestMetaData.find({
                    where: [{Tenant: tenant}, {Company: company}, {ServerType: serverType}, {RequestType: requestType}]
                }).then(function (reqMeta) {

                    if (reqMeta) {

                        getAttributeGroupData(logKey, tenant, company, JSON.parse(reqMeta.AttributeGroups)).then(function (attributeGroupData) {

                            var metaDataObj = {
                                Company: reqMeta.Company,
                                Tenant: reqMeta.Tenant,
                                ServerType: reqMeta.ServerType,
                                RequestType: reqMeta.RequestType,
                                ServingAlgo: reqMeta.ServingAlgo,
                                HandlingAlgo: reqMeta.HandlingAlgo,
                                SelectionAlgo: reqMeta.SelectionAlgo,
                                MaxReservedTime: reqMeta.MaxReservedTime,
                                MaxRejectCount: reqMeta.MaxRejectCount,
                                ReqHandlingAlgo: reqMeta.ReqHandlingAlgo,
                                ReqSelectionAlgo: reqMeta.ReqSelectionAlgo,
                                MaxAfterWorkTime: reqMeta.MaxAfterWorkTime,
                                MaxFreezeTime: reqMeta.MaxFreezeTime,
                                AttributeMeta: attributeGroupData
                            };

                            redisHandler.R_Set(logKey, metaDataKey, JSON.stringify(metaDataObj));

                            deferred.resolve(metaDataObj);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - RequestMetaDataHandler - GetMetaData - getAttributeGroupData failed :: %s', logKey, ex);
                            deferred.reject(ex);
                        });

                    } else {

                        logger.info('LogKey: %s - RequestMetaDataHandler - GetMetaData No metadata record found in database', logKey);
                        deferred.reject('No metadata record found in database');
                    }
                }).error(function (err) {

                    logger.error('LogKey: %s - RequestMetaDataHandler - GetMetaData from database failed :: %s', logKey, err);
                    deferred.reject(err);
                });

            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - RequestMetaDataHandler - GetRequestMetaData - R_Get failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestMetaDataHandler - GetRequestMetaData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getAllRequestMetaData = function (logKey, tenant, company) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RequestMetaDataHandler - GetAllRequestMetaData :: tenant: %d :: company: %d', logKey, tenant, company);

        dbConn.ArdsRequestMetaData.findAll({
            where: [{Tenant: tenant}, {Company: company}]
        }).then(function (reqMeta) {

            if (reqMeta) {

                var asyncTasks = [];

                reqMeta.forEach(function (metadata) {
                    asyncTasks.push(
                        function (callback) {
                            getAttributeGroupData(logKey, tenant, company, JSON.parse(metadata.AttributeGroups)).then(function (attributeGroupData) {

                                var metaDataObj = {
                                    Company: metadata.Company,
                                    Tenant: metadata.Tenant,
                                    ServerType: metadata.ServerType,
                                    RequestType: metadata.RequestType,
                                    ServingAlgo: metadata.ServingAlgo,
                                    HandlingAlgo: metadata.HandlingAlgo,
                                    SelectionAlgo: metadata.SelectionAlgo,
                                    MaxReservedTime: metadata.MaxReservedTime,
                                    MaxRejectCount: metadata.MaxRejectCount,
                                    ReqHandlingAlgo: metadata.ReqHandlingAlgo,
                                    ReqSelectionAlgo: metadata.ReqSelectionAlgo,
                                    MaxAfterWorkTime: metadata.MaxAfterWorkTime,
                                    MaxFreezeTime: metadata.MaxFreezeTime,
                                    AttributeMeta: attributeGroupData
                                };

                                callback(null, metaDataObj);

                            }).catch(function (ex) {

                                callback(ex, null);
                            });
                        }
                    );
                });

                async.parallel(async.reflectAll(asyncTasks), function (err, results) {

                    if (err) {

                        logger.error('LogKey: %s - RequestMetaDataHandler - GetAllRequestMetaData failed :: %s', logKey, err);
                        deferred.reject(err);
                    } else {

                        var requestMetadata = [];
                        results.forEach(function (result) {

                            if (result && !result.error && result.value) {
                                requestMetadata.push(result.value);
                            }

                        });

                        deferred.resolve(requestMetadata);
                    }

                });


            } else {

                logger.info('LogKey: %s - RequestMetaDataHandler - GetAllRequestMetaData No metadata record found in database', logKey);
                deferred.reject('No metadata record found in database');
            }
        }).error(function (err) {

            logger.error('LogKey: %s - RequestMetaDataHandler - GetAllRequestMetaData from database failed :: %s', logKey, err);
            deferred.reject(err);
        });

    } catch (ex) {

        logger.error('LogKey: %s - RequestMetaDataHandler - GetAllRequestMetaData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeRequestMetaData = function (logKey, tenant, company, serverType, requestType) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - RequestMetaDataHandler - RemoveRequestMetaData :: tenant: %d :: company: %d :: serverType: %s :: requestType: %s', logKey, tenant, company, serverType, requestType);

        dbConn.ArdsRequestMetaData.find({ where: [{ Tenant: tenant }, { Company: company }, { ServerType: serverType }, { RequestType: requestType }] }).then(function (reqMeta) {
            if (reqMeta) {
                reqMeta.destroy({ where: [{ Tenant: tenant }, { Company: company }, { ServerType: serverType }, { RequestType: requestType }] }).then(function () {

                    logger.info('LogKey: %s - RequestMetaDataHandler - Remove request metadata success :: %s', logKey);

                    var metaDataKey = util.format('ReqMETA:%d:%d:%s:%s', tenant, company, serverType, requestType);
                    redisHandler.R_Del(logKey, metaDataKey);

                    deferred.resolve('Remove request metadata success');

                }).error(function (err) {

                    logger.error('LogKey: %s - RequestMetaDataHandler - Remove request metadata failed :: %s', logKey, err);
                    deferred.reject(err);
                });
            }else{

                logger.error('LogKey: %s - RequestMetaDataHandler - No metadata record found', logKey);
                deferred.reject('No metadata record found');
            }
        }).error(function (err) {

            logger.error('LogKey: %s - RequestMetaDataHandler - Find request metadata failed :: %s', logKey, err);
            deferred.reject(err);
        });

    }catch(ex){

        logger.error('LogKey: %s - RequestMetaDataHandler - RemoveRequestMetaData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.AddRequestMetaData = addRequestMetaData;
module.exports.GetRequestMetaData = getRequestMetaData;
module.exports.GetAllRequestMetaData = getAllRequestMetaData;
module.exports.RemoveRequestMetaData = removeRequestMetaData;