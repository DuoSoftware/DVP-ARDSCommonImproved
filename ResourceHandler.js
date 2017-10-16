/**
 * Created by Heshan.i on 10/6/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var ardsMonitoringService = require('./services/ArdsMonitoringService');
var resourceStatusMapper = require('./ResourceStatusMapper');
var redisHandler = require('./RedisHandler');
var q = require('q');
var async = require('async');
var tagHandler = require('./TagHandler');
var util = require('util');


var preProcessResourceData = function (logKey, tenant, company, resourceId, handlingType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - SetResourceLogin :: tenant: %d :: company: %d :: resourceId: %s :: handlingType: %j', logKey, tenant, company, resourceId, handlingType);

        resourceService.GetResourceTaskDetails(logKey, tenant, company, resourceId).then(function (resourceTaskResult) {

            if (resourceTaskResult.IsSuccess && resourceTaskResult.Result) {

                var resourceTaskData = resourceTaskResult.Result;


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

                                deferred.resolve(taskData);

                            } else {

                                logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceAttributeDetails failed :: %s', logKey, attributeData.CustomMessage);
                                deferred.resolve(taskData);
                            }

                        }).catch(function () {

                            logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceAttributeDetails failed', logKey);
                            deferred.resolve(taskData);
                        });

                    } else {

                        logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - Assigned task not found :: %s', logKey, handlingType.Type);
                        deferred.reject('No assigned task found');
                    }

                } else {

                    logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - Invalid handling type', logKey);
                    deferred.reject('Invalid handling type');
                }

                //if (asyncTasks.length > 0) {
                //    async.parallel(async.reflectAll(asyncTasks), function (err, results) {
                //        logger.info('LogKey: %s - ResourceHandler - PreProcessResourceData :: Success', logKey);
                //
                //        var preProcessData = [];
                //        results.forEach(function (result) {
                //            if (result)
                //                preProcessData.push(result.value);
                //        });
                //
                //        deferred.resolve(preProcessData);
                //
                //    });
                //} else {
                //
                //    deferred.reject('No valid task found');
                //}

            } else {

                logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceTaskDetails failed :: %s', logKey, resourceTaskResult.CustomMessage);
                deferred.reject(resourceTaskResult.CustomMessage);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData - GetResourceTaskDetails failed', logKey);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - PreProcessResourceData failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var setResourceLogin = function (logKey, tenant, company, resourceId, userName, handlingType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - SetResourceLogin :: tenant: %d :: company: %d :: resourceId: %s :: userName: %s :: handlingType: %j', logKey, tenant, company, resourceId, userName, handlingType);

        resourceService.GetResourceDetails(logKey, tenant, company, resourceId).then(function (resourceData) {

            if (resourceData.IsSuccess && resourceData.Result) {

                var date = new Date();
                var resourceDataObj = resourceData.Result;

                preProcessResourceData(logKey, tenant, company, resourceId, handlingType).then(function (taskData) {

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
                        "tenant_" + resourceDataObj.TenantId,
                        "company_" + resourceDataObj.CompanyId,
                        "class_" + resourceDataObj.ResClass,
                        "type_" + resourceDataObj.ResType,
                        "category_" + resourceDataObj.ResCategory,
                        "resourceId_" + resourceDataObj.ResourceId,
                        "objType_Resource"
                    ];


                    var asyncTasks = [];

                    if (taskData) {
                        //--------------------Set Attribute Data--------------------------

                        taskData.AttributeData.forEach(function (attribute) {

                            var availableAttributes = resourceObj.ResourceAttributeInfo.filter(function (resourceAttribute) {
                                return resourceAttribute.Attribute === attribute.Attribute && resourceAttribute.HandlingType === attribute.HandlingType;
                            });

                            if (availableAttributes.length === 0) {
                                resourceObj.ResourceAttributeInfo.push(attribute);
                                resourceTags.push(util.format('%s:attribute_%d', taskData.HandlingType, attribute.Attribute));
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
                                                //if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length === 0) {
                                                resourceStatusMapper.SetResourceState(logKey, tenant, company, resourceId, userName, 'Available', 'Offline').then(function (result) {
                                                    callback(null, result);
                                                }).catch(function (ex) {
                                                    callback(ex, null);
                                                });
                                                //} else {
                                                //    callback(null, result);
                                                //}
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

                    } else {

                        logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - cannot proceed empty task data', logKey);
                        deferred.reject('Resource Login failed :: Cannot proceed empty task data');
                    }

                }).catch(function () {

                    logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - preProcessResourceData failed', logKey);
                    deferred.reject('Pre-Process resource data failed');
                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - GetResourceDetails failed :: %s', logKey, resourceData.CustomMessage);
                deferred.reject(resourceData.CustomMessage);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - SetResourceLogin - GetResourceDetails failed', logKey);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - SetResourceLogin failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeResource = function (logKey, tenant, company, resourceId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - RemoveResource :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        var resourceVersionKey = util.format('Version:Resource:%d:%d:%d', tenant, company, resourceId);

        redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

            if (resourceData) {

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

                    if (err) {

                        logger.error('LogKey: %s - ResourceHandler - RemoveResource failed :: %s', logKey, err);
                        deferred.reject(err);
                    } else {

                        tagHandler.RemoveTags(logKey, resourceKey).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s tag process :: %s', logKey, resourceKey, result);
                            return redisHandler.R_Del(logKey, resourceVersionKey);

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s version process :: %s', logKey, resourceVersionKey, result);
                            return redisHandler.R_Del(logKey, resourceKey);

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process :: %s', logKey, resourceKey, result);

                            var postAsyncTasks = [
                                function (callback) {
                                    var pubAdditionalParams = util.format('resourceName=%s&statusType=%s', resourceObj.ResourceName, 'removeResource');
                                    ardsMonitoringService.SendResourceStatus(logKey, tenant, company, resourceId, pubAdditionalParams).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                },
                                function (callback) {
                                    resourceStatusMapper.SetResourceState(logKey, tenant, company, resourceId, resourceObj.UserName, "NotAvailable", "UnRegister").then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }
                            ];

                            async.parallel(async.reflectAll(postAsyncTasks), function () {
                                logger.info('LogKey: %s - ResourceHandler - RemoveResource - AddResourceStatusChangeInfo :: Success', logKey);
                            });

                            deferred.resolve(result);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceHandler - RemoveResource - Remove %s process failed :: %s', logKey, resourceKey, ex);
                            deferred.reject(ex);
                        });

                    }

                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - RemoveResource - No logged in resource data found', logKey);
                deferred.reject(ex);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - RemoveResource - R_Get failed', logKey);
            deferred.reject(ex);
        })

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - RemoveResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var addResource = function (logKey, tenant, company, resourceId, username, handlingType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - AddResource :: tenant: %d :: company: %d :: resourceId: %s', logKey, tenant, company, resourceId);

        var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        redisHandler.R_Exists(logKey, resourceKey).then(function (result) {

            if (result === 1) {

                removeResource(logKey, tenant, company, resourceId).then(function (result) {

                    logger.info('LogKey: %s - ResourceHandler - AddResource - Remove existing resource success :: %s', logKey, result);
                    setResourceLogin(logKey, tenant, company, resourceId, username, handlingType).then(function (result) {

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

            } else {

                setResourceLogin(logKey, tenant, company, resourceId, username, handlingType).then(function (result) {

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


    } catch (ex) {
        logger.error('LogKey: %s - ResourceHandler - AddResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var editResource = function (logKey, tenant, company, handlingType, existingResource) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - EditResource :: tenant: %d :: company: %d :: handlingType: %j :: existingResource: %j', logKey, tenant, company, handlingType, existingResource);

        preProcessResourceData(logKey, existingResource.Tenant, existingResource.Company, existingResource.ResourceId, handlingType).then(function (taskData) {

            var asyncTasks = [];

            var newResourceTags = [
                "company_" + company,
                "tenant_" + tenant
            ];

            if (taskData) {

                var concurrencyDataKey = util.format('ConcurrencyInfo:%d:%d:%s:%s', existingResource.Tenant, existingResource.Company, existingResource.ResourceId, taskData.HandlingType);
                var concurrencyVersionKey = util.format('Version:ConcurrencyInfo:%d:%d:%s:%s', existingResource.Tenant, existingResource.Company, existingResource.ResourceId, taskData.HandlingType);

                if (existingResource.ConcurrencyInfo.indexOf(concurrencyDataKey) > -1) {
                    asyncTasks.push(
                        function (callback) {

                            redisHandler.R_Get(logKey, concurrencyDataKey).then(function (concurrencyData) {

                                if (concurrencyData) {

                                    var internalAsyncTasks = [];
                                    var concurrencyObj = JSON.parse(concurrencyData);

                                    var newConcurrencyDataTags = [
                                        "tenant_" + tenant,
                                        "company_" + company
                                    ];

                                    if (concurrencyObj.IsRejectCountExceeded) {

                                        concurrencyObj.IsRejectCountExceeded = false;
                                        concurrencyObj.RejectCount = 0;

                                        internalAsyncTasks.push(
                                            function (internalCallback) {

                                                redisHandler.R_Set(logKey, concurrencyDataKey, JSON.stringify(concurrencyObj)).then(function (concurrencyDataResult) {

                                                    logger.info('LogKey: %s - ResourceHandler - Edit concurrency data success :: %s', logKey, concurrencyDataResult);
                                                    internalCallback(null, concurrencyDataResult);
                                                }).catch(function (ex) {

                                                    logger.error('LogKey: %s - ResourceHandler - Edit concurrency data failed :: %s', logKey, ex);
                                                    internalCallback(ex, null);
                                                });

                                            }
                                        );

                                    }

                                    internalAsyncTasks.push(
                                        function (internalCallback) {

                                            tagHandler.SetTags(logKey, 'Tag:ConcurrencyInfo', newConcurrencyDataTags, concurrencyDataKey).then(function (concurrencyTagResult) {

                                                logger.info('LogKey: %s - ResourceHandler - Set concurrency tags success :: %s', logKey, concurrencyTagResult);
                                                internalCallback(null, 'Set concurrency tags success');
                                            }).catch(function (ex) {

                                                logger.error('LogKey: %s - ResourceHandler - Edit concurrency tags failed :: %s', logKey, ex);
                                                internalCallback(ex, null);
                                            });

                                        }
                                    );

                                    for (var i = 0; i < taskData.NoOfSlots; i++) {

                                        var slotDataKey = util.format('CSlotInfo:%d:%d:%s:%s:%d', existingResource.Tenant, existingResource.Company, existingResource.ResourceId, taskData.HandlingType, i);

                                        var newSlotDataTags = [
                                            "tenant_" + tenant,
                                            "company_" + company
                                        ];

                                        internalAsyncTasks.push(
                                            function (internalCallback) {

                                                tagHandler.SetTags(logKey, 'Tag:SlotInfo', newSlotDataTags, slotDataKey).then(function (slotTagResult) {

                                                    logger.info('LogKey: %s - ResourceHandler - Set slot tags success :: %s', logKey, slotTagResult);
                                                    internalCallback(null, 'Set slot tags success');
                                                }).catch(function (ex) {

                                                    logger.error('LogKey: %s - ResourceHandler - Edit slot tags failed :: %s', logKey, ex);
                                                    internalCallback(ex, null);
                                                });

                                            }
                                        );

                                    }

                                    async.parallel(internalAsyncTasks, function (err, results) {

                                        if (err) {
                                            callback(err, null);
                                        } else {
                                            callback(null, results);
                                        }

                                    });


                                } else {

                                    logger.error('LogKey: %s - ResourceHandler - EditResource - R_Get concurrency: %s :: No data found', logKey, concurrencyDataKey);
                                    callback(new Error('No concurrency data found on redis'), null);
                                }

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - ResourceHandler - EditResource - R_Get concurrency: %s data failed :: %s', logKey, concurrencyDataKey, ex);
                                callback(ex, null);
                            });

                        }
                    );

                } else {

                    //--------------------Add New Concurrency Data--------------------------
                    var date = new Date();

                    taskData.AttributeData.forEach(function (attribute) {

                        var availableAttributes = existingResource.ResourceAttributeInfo.filter(function (resourceAttribute) {
                            return resourceAttribute.Attribute === attribute.Attribute && resourceAttribute.HandlingType === attribute.HandlingType;
                        });

                        if (availableAttributes.length === 0) {
                            existingResource.ResourceAttributeInfo.push(attribute);
                            newResourceTags.push(util.format('%s:attribute_%d', taskData.HandlingType, attribute.Attribute));
                        }

                    });

                    taskData.RefInfo.ResourceId = existingResource.ResourceId.toString();
                    taskData.RefInfo.ResourceName = existingResource.ResourceName;

                    var concurrencyData = {
                        Company: existingResource.Company,
                        Tenant: existingResource.Tenant,
                        HandlingType: taskData.HandlingType,
                        LastConnectedTime: "",
                        LastRejectedSession: "",
                        RejectCount: 0,
                        MaxRejectCount: 10,
                        IsRejectCountExceeded: false,
                        ResourceId: existingResource.ResourceId,
                        UserName: existingResource.UserName,
                        ObjKey: concurrencyDataKey,
                        RefInfo: taskData.RefInfo
                    };

                    var concurrencyDataTags = [
                        "tenant_" + tenant,
                        "company_" + company,
                        "handlingType_" + taskData.HandlingType,
                        "resourceId_" + existingResource.ResourceId,
                        "objType_ConcurrencyInfo"
                    ];

                    if (existingResource.ConcurrencyInfo.indexOf(concurrencyDataKey) === -1)
                        existingResource.ConcurrencyInfo.push(concurrencyDataKey);
                    if (existingResource.LoginTasks.indexOf(taskData.HandlingType) === -1)
                        existingResource.LoginTasks.push(taskData.HandlingType);

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

                        var slotDataKey = util.format('CSlotInfo:%d:%d:%s:%s:%d', existingResource.Tenant, existingResource.Company, existingResource.ResourceId, taskData.HandlingType, i);
                        var slotVersionKey = util.format('Version:CSlotInfo:%d:%d:%s:%s:%d', existingResource.Tenant, existingResource.Company, existingResource.ResourceId, taskData.HandlingType, i);

                        var slotData = {
                            Company: existingResource.Company,
                            Tenant: existingResource.Tenant,
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
                            ResourceId: existingResource.ResourceId,
                            SlotId: i,
                            ObjKey: slotDataKey,
                            OtherInfo: "",
                            EnableToProductivity: taskData.EnableToProductivity
                        };

                        var slotDataTags = [
                            "tenant_" + tenant,
                            "company_" + company,
                            "handlingType_" + taskData.HandlingType,
                            "state_Available",
                            "resourceId_" + existingResource.ResourceId,
                            "slotId_" + i,
                            "objType_CSlotInfo"
                        ];

                        if (existingResource.ConcurrencyInfo.indexOf(slotDataKey) === -1)
                            existingResource.ConcurrencyInfo.push(slotDataKey);

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

                }

                async.parallel(asyncTasks, function (err, results) {

                    if (err) {

                        logger.error('LogKey: %s - ResourceHandler - EditResource - Error occurred in edit resource', logKey);
                        deferred.reject('Error occurred in edit resource');
                    } else {

                        var resourceKey = util.format('Resource:%d:%d:%d', existingResource.Tenant, existingResource.Company, existingResource.ResourceId);

                        redisHandler.R_Set(logKey, resourceKey, JSON.stringify(existingResource)).then(function (resourceDataResult) {

                            logger.info('LogKey: %s - ResourceHandler - Set resource data success :: %s', logKey, resourceDataResult);
                            return tagHandler.SetTags(logKey, 'Tag:Resource', newResourceTags, resourceKey);

                        }).then(function (resourceTagResult) {

                            logger.info('LogKey: %s - ResourceHandler - Set resource tags success :: %s', logKey, resourceTagResult);
                            logger.info('LogKey: %s - ResourceHandler - Edit Resource success :: %s', logKey, resourceKey);

                            ardsMonitoringService.SendResourceStatus(logKey, tenant, company, existingResource.ResourceId, null);

                            deferred.resolve('Edit resource success');

                        }).catch(function (ex) {
                            logger.error('LogKey: %s - ResourceHandler - EditResource - set resource data failed :: %s', logKey, ex);
                            deferred.reject('Edit resource failed :: Edit resource data');
                        });

                    }

                });


            } else {

                logger.error('LogKey: %s - ResourceHandler - EditResource - cannot proceed empty task data', logKey);
                deferred.reject('Cannot proceed empty task data');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - EditResource - PreProcessResourceData failed:: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - EditResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var shareResource = function (logKey, tenant, company, resourceId, userName, handlingType) {
    var deferred = q.defer();

    try {

        var resourceSearchTags = [
            'Tag:Resource:resourceId_' + resourceId,
            'Tag:Resource:objType_Resource'
        ];

        redisHandler.R_SInter(logKey, resourceSearchTags).then(function (resourceKeys) {

            if (resourceKeys && resourceKeys.length > 0) {

                var resourceKey = resourceKeys[0];
                redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

                    if (resourceData) {

                        var resourceObj = JSON.parse(resourceData);
                        return editResource(logKey, tenant, company, handlingType, resourceObj);

                    } else {

                        logger.error('LogKey: %s - ResourceHandler - ShareResource - R_Get Resource :: %s failed:: No resource data found', logKey, resourceKey);
                        deferred.reject('Get resource data failed');
                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - ShareResource - R_Get Resource :: %s failed:: %s', logKey, resourceKey, ex);
                    deferred.reject('Get resource data failed');
                });

            } else {

                return setResourceLogin(logKey, tenant, company, resourceId, userName, handlingType);
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - ShareResource - R_SInter Resource failed:: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - ShareResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeShareResource = function (logKey, tenant, company, resourceId, handlingType) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - RemoveShareResource :: tenant: %d :: company: %d :: handlingType: %j :: resourceId: %s', logKey, tenant, company, handlingType, resourceId);

        var resourceSearchTags = [
            'Tag:Resource:resourceId_' + resourceId,
            'Tag:Resource:objType_Resource'
        ];

        redisHandler.R_SInter(logKey, resourceSearchTags).then(function (resourceKeys) {

            if (resourceKeys && resourceKeys.length > 0) {

                var resourceKey = resourceKeys[0];
                redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

                    if (resourceData) {

                        var resourceObj = JSON.parse(resourceData);

                        preProcessResourceData(logKey, tenant, company, resourceId, handlingType).then(function (taskData) {

                            var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
                            var tagReferenceKey = util.format('TagReference:Resource:%d:%d:%d', tenant, company, resourceId);
                            redisHandler.R_SMembers(logKey, tagReferenceKey).then(function (tagReferenceData) {

                                var resourceTenantTagCount = 0;
                                var resourceCompanyTagCount = 0;

                                if (tagReferenceData) {

                                    tagReferenceData.forEach(function (tagRefValue) {
                                        if (tagRefValue.startsWith('Tag:Resource:tenant_'))
                                            resourceTenantTagCount++;

                                        if (tagRefValue.startsWith('Tag:Resource:company_'))
                                            resourceCompanyTagCount++;
                                    });
                                }

                                if (resourceTenantTagCount < 2 && resourceCompanyTagCount < 2) {

                                    var asyncTasks = [];
                                    var concurrencyRemoveIndexes = [];
                                    var attributeRemoveIndexes = [];
                                    var attributeRemoveTags = [];

                                    var loginTaskRemoveIndex = resourceObj.LoginTasks.indexOf(taskData.HandlingType);

                                    resourceObj.ConcurrencyInfo.forEach(function (concurrencyKey, i) {
                                        if (concurrencyKey.indexOf(taskData.HandlingType) > -1) {

                                            var concurrencyVersionKey = util.format('Version:%s', concurrencyKey);
                                            concurrencyRemoveIndexes.push(i);

                                            asyncTasks.push(
                                                function (callback) {

                                                    tagHandler.RemoveTags(logKey, concurrencyKey).then(function (result) {

                                                        logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Remove %s tag process :: %s', logKey, concurrencyKey, result);
                                                        return redisHandler.R_Del(logKey, concurrencyVersionKey);

                                                    }).then(function (result) {

                                                        logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Remove %s version process :: %s', logKey, concurrencyVersionKey, result);
                                                        return redisHandler.R_Del(logKey, concurrencyKey);

                                                    }).then(function (result) {

                                                        logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Remove %s process :: %s', logKey, concurrencyKey, result);
                                                        callback(null, result);

                                                    }).catch(function (ex) {

                                                        logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - Remove %s process failed :: %s', logKey, concurrencyKey, ex);
                                                        callback(ex, null);
                                                    });

                                                }
                                            );
                                        }
                                    });

                                    resourceObj.ResourceAttributeInfo.forEach(function (attributeData, i) {
                                        if (attributeData.HandlingType === taskData.HandlingType) {
                                            attributeRemoveIndexes.push(i);

                                            attributeRemoveTags.push(
                                                {
                                                    TagKey: util.format('Tag:Resource:%s:attribute_%d', taskData.HandlingType, attributeData.Attribute),
                                                    TagValue: resourceKey,
                                                    TagReference: tagReferenceKey
                                                }
                                            );
                                        }
                                    });

                                    async.parallel(asyncTasks, function (err) {
                                        if (err) {

                                            logger.error('LogKey: %s - ResourceHandler - RemoveShareResource failed :: %s', logKey, err);
                                            deferred.reject(err);
                                        } else {

                                            resourceObj.LoginTasks.splice(loginTaskRemoveIndex, 1);

                                            concurrencyRemoveIndexes.reverse().forEach(function (concurrencyIndex) {
                                                resourceObj.ConcurrencyInfo.splice(concurrencyIndex, 1);
                                            });

                                            attributeRemoveIndexes.reverse().forEach(function (attributeIndex) {
                                                resourceObj.ResourceAttributeInfo.splice(attributeIndex, 1);
                                            });

                                            tagHandler.RemoveSpecificTags(logKey, attributeRemoveTags).then(function (result) {

                                                logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Remove %s tag process :: %s', logKey, resourceKey, result);
                                                return redisHandler.R_Set(logKey, resourceKey, JSON.stringify(resourceObj));
                                            }).then(function (result) {

                                                logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Edit %s process :: %s', logKey, resourceKey, result);
                                                deferred.resolve(result);
                                            }).catch(function (ex) {

                                                logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - Edit %s process failed :: %s', logKey, resourceKey, ex);
                                                deferred.reject(ex);
                                            });

                                        }
                                    });


                                } else {

                                    // Remove task sharing information
                                    var tagsToRemove = [];
                                    var concurrencyKey = util.format('ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, taskData.HandlingType);
                                    var concurrencyTagReference = util.format('TagReference:ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, taskData.HandlingType);

                                    if (resourceTenantTagCount >= 2) {

                                        tagsToRemove.push(
                                            {
                                                TagKey: util.format('Tag:Resource:tenant_%d', tenant),
                                                TagValue: resourceKey,
                                                TagReference: tagReferenceKey
                                            }
                                        );
                                        tagsToRemove.push(
                                            {
                                                TagKey: util.format('Tag:ConcurrencyInfo:tenant_%d', tenant),
                                                TagValue: concurrencyKey,
                                                TagReference: concurrencyTagReference
                                            }
                                        );
                                        for (var i = 0; i < taskData.NoOfSlots; i++) {
                                            var slotKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, taskData.HandlingType, i);
                                            var slotTagReference = util.format('TagReference:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, taskData.HandlingType, i);

                                            tagsToRemove.push(
                                                {
                                                    TagKey: util.format('Tag:SlotInfo:tenant_%d', tenant),
                                                    TagValue: slotKey,
                                                    TagReference: slotTagReference
                                                }
                                            );
                                        }
                                    }

                                    if (resourceCompanyTagCount >= 2) {

                                        tagsToRemove.push(
                                            {
                                                TagKey: util.format('Tag:Resource:company_%d', company),
                                                TagValue: resourceKey,
                                                TagReference: tagReferenceKey
                                            }
                                        );
                                        tagsToRemove.push(
                                            {
                                                TagKey: util.format('Tag:Resource:company_%d', company),
                                                TagValue: concurrencyKey,
                                                TagReference: concurrencyTagReference
                                            }
                                        );
                                        for (var j = 0; j < taskData.NoOfSlots; j++) {
                                            var slotKey2 = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, taskData.HandlingType, i);
                                            var slotTagReference2 = util.format('TagReference:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, taskData.HandlingType, i);

                                            tagsToRemove.push(
                                                {
                                                    TagKey: util.format('Tag:SlotInfo:company_%d', company),
                                                    TagValue: slotKey2,
                                                    TagReference: slotTagReference2
                                                }
                                            );
                                        }
                                    }

                                    tagHandler.RemoveSpecificTags(logKey, tagsToRemove).then(function () {

                                        logger.info('LogKey: %s - ResourceHandler - RemoveShareResource - Remove sharing success', logKey);
                                        deferred.reject('Remove sharing success');
                                    }).catch(function () {

                                        logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - Remove sharing failed', logKey);
                                        deferred.reject('Remove sharing failed');
                                    });

                                }

                            }).catch(function (ex) {

                                logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - Search tag reference failed :: %s', logKey, ex);
                                deferred.reject('Search tag reference failed');
                            });

                        }).catch(function () {

                            logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - preProcessResourceData failed', logKey);
                            deferred.reject('Pre-Process resource data failed');
                        });

                    } else {

                        logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - R_Get Resource :: %s failed:: No resource data found', logKey, resourceKey);
                        deferred.reject('Get resource data failed');
                    }

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - ShareResource - R_Get Resource :: %s failed:: %s', logKey, resourceKey, ex);
                    deferred.reject('Get resource data failed');
                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - R_SInter Resource :: %s failed:: No logged in resource found', logKey, resourceId);
                deferred.reject('No logged in resource found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - RemoveShareResource - R_SInter Resource failed:: %s', logKey, ex);
            deferred.reject(ex);
        });
    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - RemoveShareResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.RemoveResource = removeResource;
module.exports.AddResource = addResource;
module.exports.ShareResource = shareResource;
module.exports.RemoveShareResource = removeShareResource;