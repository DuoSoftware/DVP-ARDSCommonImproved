/**
 * Created by Heshan.i on 10/6/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var resourceStatusMapper = require('./ResourceStatusMapper');
var redisHandler = require('./RedisHandler');
var q = require('q');
var async = require('async');
var tagHandler = require('./TagHandler');
var util = require('util');


var preProcessResourceData = function (logKey, tenant, company, resourceId, handlingTypes) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - SetResourceLogin :: tenant: %d :: company: %d :: resourceId: %j :: handlingTypes: %j', logKey, tenant, company, resourceId, handlingTypes);

        resourceService.GetResourceTaskDetails(logKey, tenant, company, resourceId).then(function (resourceTaskResult) {

            if (resourceTaskResult.IsSuccess && resourceTaskResult.Result) {

                var asyncTasks = [];
                var resourceTaskData = resourceTaskResult.Result;
                handlingTypes.forEach(function (handlingType) {

                    asyncTasks.push(
                        function (callback) {

                            if (handlingType.Type) {

                                var availableHandlingType = resourceTaskData.filter(function (resourceTask) {
                                    return resourceTask.ResTask.ResTaskInfo.TaskType === handlingType.Type;
                                });

                                if (availableHandlingType && availableHandlingType.length > 0) {

                                    var matchingTask = availableHandlingType[0];
                                    var taskData = {
                                        HandlingType: matchingTask.ResTask.ResTaskInfo.TaskType,
                                        EnableToProductivity: matchingTask.ResTask.AddToProductivity,
                                        NoOfSlots: matchingTask.Concurrency,
                                        RefInfo: handlingType.Contact ? handlingType.Contact : matchingTask.RefInfo,
                                        AttributeData: []
                                    };

                                    resourceService.GetResourceAttributeDetails(logKey, tenant, company, matchingTask.ResTaskId).then(function (attributeData) {

                                        if (attributeData.IsSuccess && attributeData.Result) {

                                            var resourceAttributeData = attributeData.Result.ResResourceAttributeTask;
                                            resourceAttributeData.forEach(function (resourceAttribute) {

                                                if (resourceAttribute && resourceAttribute.Percentage && resourceAttribute.Percentage > 0) {
                                                    var attribute = {
                                                        Attribute: resourceAttribute.AttributeId.toString(),
                                                        HandlingType: handlingType.Type,
                                                        Percentage: resourceAttribute.Percentage
                                                    };

                                                    taskData.AttributeData.push(attribute);
                                                }

                                            });

                                            callback(null, taskData);

                                        } else {

                                            logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceAttributeDetails failed :: %s', logKey, attributeData.CustomMessage);
                                            callback(null, taskData);
                                        }

                                    }).catch(function () {

                                        logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceAttributeDetails failed', logKey);
                                        callback(null, taskData);
                                    });

                                } else {

                                    logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - Assigned task not found :: %s', logKey, handlingType.Type);
                                    callback(new Error('Assigned task not found'), null);
                                }

                            } else {

                                logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - Invalid handling type', logKey);
                                callback(new Error('Invalid handling type'), null);
                            }

                        }
                    );

                });

                if (asyncTasks.length > 0) {
                    async.parallel(async.reflectAll(asyncTasks), function (err, results) {
                        logger.info('LogKey: %s - ResourceHandler - PreProcessResourceData :: Success', logKey);

                        var preProcessData = [];
                        results.forEach(function (result) {
                            if (result)
                                preProcessData.push(result.value);
                        });

                        deferred.resolve(preProcessData);

                    });
                } else {

                    deferred.reject('No valid task found');
                }

            } else {

                logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceTaskDetails failed :: %s', logKey, resourceTaskResult.CustomMessage);
                deferred.reject(resourceTaskResult.CustomMessage);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceTaskDetails failed', logKey);
            deferred.reject(ex.message);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData failed :: %s', logKey, ex);
        deferred.reject(ex.message);
    }

    return deferred.promise;
};

var setResourceLogin = function (logKey, tenant, company, resourceId, userName, handlingTypes) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - SetResourceLogin :: tenant: %d :: company: %d :: resourceId: %s :: userName: %s :: handlingTypes: %j', logKey, tenant, company, resourceId, userName, handlingTypes);

        resourceService.GetResourceDetails(logKey, tenant, company, resourceId).then(function (resourceData) {

            if (resourceData.IsSuccess && resourceData.Result) {

                var date = new Date();
                var resourceDataObj = resourceData.Result;

                preProcessResourceData(logKey, tenant, company, resourceId, handlingTypes).then(function (preProcessData) {

                    var resourceKey = util.format('Resource:%d:%d:%d', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId);
                    var resourceVersionKey = util.format('Version:Resource:%d:%d:%d', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId);
                    var resourceIssMapKey = util.format('ResourceIssMap:%d:%d:%s', resourceDataObj.TenantId, resourceDataObj.CompanyId, userName);

                    var resourceObj = {
                        Company: resourceDataObj.CompanyId,
                        Tenant: resourceDataObj.TenantId,
                        Class: resourceDataObj.ResClass,
                        Type: resourceDataObj.ResType,
                        Category: resourceDataObj.ResCategory,
                        ResourceId: resourceDataObj.ResourceId,
                        ResourceName: resourceDataObj.ResourceName,
                        UserName: userName,
                        ResourceAttributeInfo: [],
                        ConcurrencyInfo: [],
                        LoginTasks: [],
                        OtherInfo: resourceDataObj.OtherData
                    };

                    var resourceTags = [
                        "company_" + resourceDataObj.CompanyId,
                        "tenant_" + resourceDataObj.TenantId,
                        "class_" + resourceDataObj.ResClass,
                        "type_" + resourceDataObj.ResType,
                        "category_" + resourceDataObj.ResCategory,
                        "resourceid_" + resourceDataObj.ResourceId,
                        "objtype_Resource"
                    ];


                    var asyncTasks = [];

                    preProcessData.forEach(function (taskData) {

                        if (taskData) {
                            //--------------------Set Attribute Data--------------------------

                            taskData.AttributeData.forEach(function (attribute) {

                                var availableAttributes = resourceObj.ResourceAttributeInfo.filter(function (resourceAttribute) {
                                    return resourceAttribute.Attribute === attribute.Attribute && resourceAttribute.HandlingType === attribute.HandlingType;
                                });

                                if (availableAttributes.length === 0) {
                                    resourceObj.ResourceAttributeInfo.push(attribute);
                                    resourceTags.push('attribute_' + attribute.Attribute);
                                }

                            });


                            //--------------------Set Concurrency Data--------------------------

                            var concurrencyDataKey = util.format('ConcurrencyInfo:%d:%d:%s:%s', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId, taskData.HandlingType);
                            var concurrencyVersionKey = util.format('Version:ConcurrencyInfo:%d:%d:%s:%s', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId, taskData.HandlingType);

                            taskData.RefInfo.ResourceId = resourceData.Result.ResourceId.toString();
                            taskData.RefInfo.ResourceName = resourceData.Result.ResourceName;

                            var concurrencyData = {
                                Company: resourceDataObj.CompanyId,
                                Tenant: resourceDataObj.TenantId,
                                HandlingType: taskData.HandlingType,
                                LastConnectedTime: "",
                                LastRejectedSession: "",
                                RejectCount: 0,
                                MaxRejectCount: 10,
                                IsRejectCountExceeded: false,
                                ResourceId: resourceDataObj.ResourceId,
                                UserName: userName,
                                ObjKey: concurrencyDataKey,
                                RefInfo: taskData.RefInfo
                            };

                            var concurrencyDataTags = [
                                "tenant_" + resourceDataObj.TenantId,
                                "company_" + resourceDataObj.CompanyId,
                                "handlingType_" + taskData.HandlingType,
                                "resourceId_" + resourceDataObj.ResourceId,
                                "objType_ConcurrencyInfo"
                            ];

                            if (resourceObj.ConcurrencyInfo.indexOf(concurrencyDataKey) === -1)
                                resourceObj.ConcurrencyInfo.push(concurrencyDataKey);
                            if (resourceObj.LoginTasks.indexOf(taskData.HandlingType) === -1)
                                resourceObj.LoginTasks.push(taskData.HandlingType);

                            asyncTasks.push(
                                function (callback) {

                                    redisHandler.R_Set(logKey, concurrencyVersionKey, '0').then(function (versionResult) {

                                        logger.info('LogKey: %s - ResourceHandler - Set concurrency version success :: %s', logKey, versionResult);
                                        return redisHandler.R_Set(logKey, concurrencyDataKey, JSON.stringify(concurrencyData));

                                    }).then(function (concurrencyDataResult) {

                                        logger.info('LogKey: %s - ResourceHandler - Set concurrency data success :: %s', logKey, concurrencyDataResult);
                                        return tagHandler.SetTags(logKey, 'Tag:ConcurrencyInfo', concurrencyDataTags, concurrencyDataKey);

                                    }).then(function (concurrencyTagResult) {

                                        logger.info('LogKey: %s - ResourceHandler - Set concurrency tags success :: %s', logKey, concurrencyTagResult);
                                        callback(null, 'Set concurrency data success');

                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });

                                }
                            );


                            //--------------------Set Slot Data--------------------------

                            for (var i = 0; i < taskData.NoOfSlots; i++) {

                                var slotDataKey = util.format('CSlotInfo:%d:%d:%s:%s:%d', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId, taskData.HandlingType, i);
                                var slotVersionKey = util.format('Version:CSlotInfo:%d:%d:%s:%s:%d', resourceDataObj.TenantId, resourceDataObj.CompanyId, resourceDataObj.ResourceId, taskData.HandlingType, i);

                                var slotData = {
                                    Company: resourceDataObj.CompanyId,
                                    Tenant: resourceDataObj.TenantId,
                                    HandlingType: taskData.HandlingType,
                                    State: "Available",
                                    StateChangeTime: date.toISOString(),
                                    HandlingRequest: "",
                                    LastReservedTime: "",
                                    MaxReservedTime: 10,
                                    MaxAfterWorkTime: 0,
                                    MaxFreezeTime: 0,
                                    FreezeAfterWorkTime: false,
                                    TempMaxRejectCount: 10,
                                    ResourceId: resourceDataObj.ResourceId,
                                    SlotId: i,
                                    ObjKey: slotDataKey,
                                    OtherInfo: "",
                                    EnableToProductivity: taskData.EnableToProductivity
                                };

                                var slotDataTags = [
                                    "tenant_" + resourceDataObj.TenantId,
                                    "company_" + resourceDataObj.CompanyId,
                                    "handlingType_" + taskData.HandlingType,
                                    "state_Available",
                                    "resourceId_" + resourceDataObj.ResourceId,
                                    "slotId_" + i,
                                    "objType_CSlotInfo"
                                ];

                                if (resourceObj.ConcurrencyInfo.indexOf(slotDataKey) === -1)
                                    resourceObj.ConcurrencyInfo.push(slotDataKey);

                                asyncTasks.push(
                                    function (callback) {

                                        redisHandler.R_Set(logKey, slotVersionKey, '0').then(function (versionResult) {

                                            logger.info('LogKey: %s - ResourceHandler - Set slot version success :: %s', logKey, versionResult);
                                            return redisHandler.R_Set(logKey, slotDataKey, JSON.stringify(slotData));

                                        }).then(function (slotDataResult) {

                                            logger.info('LogKey: %s - ResourceHandler - Set slot data success :: %s', logKey, slotDataResult);
                                            return tagHandler.SetTags(logKey, 'Tag:SlotInfo', slotDataTags, slotDataKey);

                                        }).then(function (slotTagResult) {

                                            logger.info('LogKey: %s - ResourceHandler - Set slot tags success :: %s', logKey, slotTagResult);
                                            callback(null, 'Set slot data success');

                                        }).catch(function (ex) {
                                            callback(ex, null);
                                        });

                                    }
                                );

                            }

                        } else {

                            logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - cannot proceed empty task data', logKey);
                        }

                    });

                    async.parallel(asyncTasks, function (err) {
                        if (err) {

                            logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - set concurrency data failed :: %s', logKey, err);
                            deferred.reject('Resource Login failed :: Set concurrency data');

                        } else {

                            redisHandler.R_Set(logKey, resourceVersionKey, '0').then(function (versionResult) {

                                logger.info('LogKey: %s - ResourceHandler - Set resource version success :: %s', logKey, versionResult);
                                return redisHandler.R_Set(logKey, resourceKey, JSON.stringify(resourceObj));

                            }).then(function (resourceDataResult) {

                                logger.info('LogKey: %s - ResourceHandler - Set resource data success :: %s', logKey, resourceDataResult);
                                return tagHandler.SetTags(logKey, 'Tag:Resource', resourceTags, resourceKey);

                            }).then(function (resourceTagResult) {

                                logger.info('LogKey: %s - ResourceHandler - Set resource tags success :: %s', logKey, resourceTagResult);
                                logger.info('LogKey: %s - ResourceHandler - Set Resource login success :: %s', logKey, resourceKey);

                                var postAsyncTasks = [
                                    function (callback) {
                                        redisHandler.R_SetNx(logKey, resourceIssMapKey, resourceKey).then(function (result) {
                                            callback(null, result);
                                        }).catch(function (ex) {
                                            callback(ex, null);
                                        });
                                    },
                                    function (callback) {
                                        resourceStatusMapper.SetResourceState(logKey, tenant, company, resourceId, userName, 'Available', 'Register').then(function (result) {
                                            if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length === 0) {
                                                resourceStatusMapper.SetResourceState(logKey, tenant, company, resourceId, userName, "Available", "Offline").then(function (result) {
                                                    callback(null, result);
                                                }).catch(function (ex) {
                                                    callback(ex, null);
                                                });
                                            }else{
                                                callback(null, result);
                                            }
                                        }).catch(function (ex) {
                                            callback(ex, null);
                                        });
                                    }
                                ];

                                async.parallel(async.reflectAll(postAsyncTasks), function () {
                                    logger.info('LogKey: %s - ResourceHandler - AddResourceStatusDurationInfo :: Success', logKey);
                                });

                                deferred.resolve('Resource login success');

                            }).catch(function (ex) {
                                logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - set resource data failed :: %s', logKey, ex);
                                deferred.reject('Resource Login failed :: Set resource data');
                            });
                        }
                    });

                }).catch(function () {

                    logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - preProcessResourceData failed', logKey);
                    deferred.reject(resourceData.CustomMessage);
                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - GetResourceDetails failed :: %s', logKey, resourceData.CustomMessage);
                deferred.reject(resourceData.CustomMessage);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - GetResourceDetails failed', logKey);
            deferred.reject(ex.message);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - SetResourceLogin failed :: %s', logKey, ex);
        deferred.reject(ex.message);
    }

    return deferred.promise;
};

var removeResource = function (logKey, tenant, company, resourceId) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - ResourceHandler - RemoveResource :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        var resourceVersionKey = util.format('Version:Resource:%d:%d:%d', tenant, company, resourceId);
        
        redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

            if(resourceData){

                var resourceObj = JSON.parse(resourceData);

                var asyncTasks = [];
                
                resourceObj.ConcurrencyInfo.forEach(function (concurrencyKey) {

                    var concurrencyVersionKey = util.format('Version:%s', concurrencyKey);

                    asyncTasks.push(
                        function (callback) {

                            tagHandler.RemoveTags(logKey, concurrencyKey).then(function (result) {

                                logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s tag process :: %s', logKey, concurrencyKey, result);
                                return redisHandler.R_Del(logKey, concurrencyVersionKey);

                            }).then(function (result) {

                                logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s version process :: %s', logKey, concurrencyVersionKey, result);
                                return redisHandler.R_Del(logKey, concurrencyKey);

                            }).then(function (result) {

                                logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process :: %s', logKey, concurrencyKey, result);
                                callback(null, result);

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process failed :: %s', logKey, concurrencyKey, ex);
                                callback(ex, null);
                            });

                        }  
                    );
                });

                async.parallel(asyncTasks, function (err) {

                    if(err){

                        logger.error('LogKey: %s - ResourceHandler - RemoveResource failed :: %s', logKey, err);
                        deferred.reject(err);
                    }else{

                        tagHandler.RemoveTags(logKey, resourceKey).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s tag process :: %s', logKey, resourceKey, result);
                            return redisHandler.R_Del(logKey, resourceVersionKey);

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s version process :: %s', logKey, resourceVersionKey, result);
                            return redisHandler.R_Del(logKey, resourceKey);

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process :: %s', logKey, resourceKey, result);
                            deferred.resolve(result);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process failed :: %s', logKey, resourceKey, ex);
                            deferred.reject(ex);
                        });

                    }

                });

            }else{

                logger.error('LogKey: %s - ResourceHandler - RemoveResource - No logged in resource data found', logKey);
                deferred.reject(ex.message);
            }
            
        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - RemoveResource - R_Get failed', logKey);
            deferred.reject(ex.message);
        })

    }catch(ex){

        logger.error('LogKey: %s - ResourceHandler - RemoveResource failed :: %s', logKey, ex);
        deferred.reject(ex.message);
    }

    return deferred.promise;
};

var addResource = function (logKey, tenant, company, resourceId, username, handlingTypes) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - ResourceHandler - AddResource :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        redisHandler.R_Exists(logKey, resourceKey).then(function (result) {

            if(result === 1){

                removeResource(logKey, tenant, company, resourceId).then(function (result) {

                    logger.info('LogKey: %s - ResourceHandler - AddResource - Remove existing resource success :: %s', logKey, result);
                    setResourceLogin(logKey, tenant, company, resourceId, username, handlingTypes).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - AddResource - Set resource login success :: %s', logKey, result);
                        deferred.resolve('Set resource login success');

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - AddResource - Set resource login failed :: %s', logKey, ex);
                        deferred.reject('Set resource login failed');
                    });

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - AddResource - Remove existing resource failed :: %s', logKey, ex);
                    deferred.reject('Remove existing resource failed');
                });

            }else{

                setResourceLogin(logKey, tenant, company, resourceId, username, handlingTypes).then(function (result) {

                    logger.info('LogKey: %s - ResourceHandler - AddResource - Set resource login success :: %s', logKey, result);
                    deferred.resolve('Set resource login success');

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - AddResource - Set resource login failed :: %s', logKey, ex);
                    deferred.reject('Set resource login failed');
                });
            }

        }).catch(function (ex) {
            logger.error('LogKey: %s - ResourceHandler - AddResource - R_Exists failed', logKey);
            deferred.reject(ex);
        })


    }catch(ex){
        logger.error('LogKey: %s - ResourceHandler - AddResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.RemoveResource = removeResource;
module.exports.AddResource = addResource;