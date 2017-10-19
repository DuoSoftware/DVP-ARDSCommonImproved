/**
 * Created by Heshan.i on 10/6/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var resourceService = require('./services/ResourceService');
var ardsMonitoringService = require('./services/ArdsMonitoringService');
var notificationService = require('./services/NotificationService');
var scheduleWorkerService = require('./services/ScheduleWorkerService');
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
                                        logger.info('LogKey: %s - ResourceHandler - AddResourceStatusChangeInfo :: Success', logKey);
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

                        preProcessResourceData(logKey, resourceObj.Tenant, resourceObj.Company, resourceId, handlingType).then(function (taskData) {

                            var resourceKey = util.format('Resource:%d:%d:%d', resourceObj.Tenant, resourceObj.Company, resourceId);
                            var tagReferenceKey = util.format('TagReference:Resource:%d:%d:%d', resourceObj.Tenant, resourceObj.Company, resourceId);
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
                                    var concurrencyKey = util.format('ConcurrencyInfo:%d:%d:%d:%s', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType);
                                    var concurrencyTagReference = util.format('TagReference:ConcurrencyInfo:%d:%d:%d:%s', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType);

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
                                            var slotKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType, i);
                                            var slotTagReference = util.format('TagReference:CSlotInfo:%d:%d:%d:%s:%d', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType, i);

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
                                                TagKey: util.format('Tag:ConcurrencyInfo:company_%d', company),
                                                TagValue: concurrencyKey,
                                                TagReference: concurrencyTagReference
                                            }
                                        );
                                        for (var j = 0; j < taskData.NoOfSlots; j++) {
                                            var slotKey2 = util.format('CSlotInfo:%d:%d:%d:%s:%d', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType, j);
                                            var slotTagReference2 = util.format('TagReference:CSlotInfo:%d:%d:%d:%s:%d', resourceObj.Tenant, resourceObj.Company, resourceId, taskData.HandlingType, j);

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

var setResourceAttributes = function (logKey, tenant, company, resourceId, newAttribute) {
    var deferred = q.defer();

    try {

        var resourceKey = util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

            if (resourceData) {

                var resourceObj = JSON.parse(resourceData);

                var attributeIndex = -1;
                resourceObj.ResourceAttributeInfo.forEach(function (attribute, i) {
                    if (attribute.Attribute === newAttribute.Attribute && attribute.HandlingType === newAttribute.HandlingType)
                        attributeIndex = i;
                });

                var newAttributeTag = [];
                if (attributeIndex > -1) {

                    resourceObj.ResourceAttributeInfo[attributeIndex] = newAttribute;
                } else {

                    resourceObj.ResourceAttributeInfo.push(newAttribute);
                    newAttributeTag.push(util.format('%s:attribute_%d', newAttribute.HandlingType, newAttribute.Attribute))
                }


                redisHandler.R_Set(logKey, resourceKey, JSON.stringify(resourceObj)).then(function (result) {

                    logger.info('LogKey: %s - ResourceHandler - Set resource attribute success :: %s', logKey, result);
                    return tagHandler.SetTags(logKey, 'Tag:Resource', newAttributeTag, resourceKey);
                }).then(function (resourceTagResult) {

                    logger.info('LogKey: %s - ResourceHandler - Set resource tags success :: %s', logKey, resourceTagResult);
                    deferred.resolve('Set resource attribute success');
                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - SetResourceAttributes - R_Set Resource : %s failed :: %s', logKey, resourceKey, ex);
                    deferred.reject('Set Resource Attribute failed');
                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - SetResourceAttributes - No logged in resource found', logKey);
                deferred.reject('No logged in resource found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - SetResourceAttributes - R_Get :: failed :: %s', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {
        logger.error('LogKey: %s - ResourceHandler - SetResourceAttributes failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getResource = function (logKey, tenant, company, resourceId, resourceKey) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - GetResource :: tenant: %d :: company: %d :: resourceId: %d :: resourceKey: %s', logKey, tenant, company, resourceId, resourceKey);

        resourceKey = (resourceKey) ? resourceKey : util.format('Resource:%d:%d:%d', tenant, company, resourceId);
        redisHandler.R_Get(logKey, resourceKey).then(function (resourceData) {

            if (resourceData) {

                var resourceObj = JSON.parse(resourceData);

                if (resourceObj.ConcurrencyInfo && resourceObj.ConcurrencyInfo.length > 0) {

                    redisHandler.R_MGet(logKey, resourceObj.ConcurrencyInfo).then(function (concurrencyData) {

                        if (concurrencyData && concurrencyData.length > 0) {

                            var concurrencyObjects = [];
                            var slotObjects = [];
                            concurrencyData.forEach(function (concurrency) {

                                var concurrencyObj = JSON.parse(concurrency);
                                if (concurrencyObj.ObjKey.indexOf('ConcurrencyInfo') > -1)
                                    concurrencyObjects.push(concurrencyObj);

                                if (concurrencyObj.ObjKey.indexOf('CSlotInfo') > -1)
                                    slotObjects.push(concurrencyObj);

                            });

                            concurrencyObjects.forEach(function (task) {

                                task.SlotInfo = slotObjects.filter(function (slot) {
                                    return slot.HandlingType === task.HandlingType;
                                });

                            });

                            resourceObj.ConcurrencyInfo = concurrencyObjects;

                            logger.info('LogKey: %s - ResourceHandler - GetResource - success :: %j', logKey, resourceObj);
                            deferred.resolve(resourceObj);

                        } else {

                            logger.info('LogKey: %s - ResourceHandler - GetResource - R_MGet: %j :: No concurrency data found', logKey, resourceObj.ConcurrencyInfo);
                            deferred.resolve(resourceObj);
                        }

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - GetResource - R_MGet: %j :: %s', logKey, resourceObj.ConcurrencyInfo, ex);
                        deferred.resolve(resourceObj);
                    });

                }


            } else {

                logger.error('LogKey: %s - ResourceHandler - GetResource - R_Get: %s :: No resource data found', logKey, resourceKey);
                deferred.reject('No resource data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - GetResource - R_Get: %s failed', logKey, resourceKey);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - GetResource failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getResourceStatus = function (logKey, tenant, company, resourceId) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - GetResourceStatus :: tenant: %d :: company: %d :: resourceId: %d', logKey, tenant, company, resourceId);

        var resourceStatusKey = util.format('ResourceState:%d:%d:%d', tenant, company, resourceId);
        redisHandler.R_Get(logKey, resourceStatusKey).then(function (resourceStatusData) {

            if (resourceStatusData) {

                var resourceStatusObj = JSON.parse(resourceStatusData);

                logger.info('LogKey: %s - ResourceHandler - GetResourceStatus - success :: %j', logKey, resourceStatusObj);
                deferred.resolve(resourceStatusObj);

            } else {

                logger.error('LogKey: %s - ResourceHandler - GetResourceStatus - R_Get: %s :: No resource status data found', logKey, resourceKey);
                deferred.reject('No resource status data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - GetResourceStatus - R_Get: %s failed', logKey, resourceKey);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - GetResourceStatus failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var getResourcesByTags = function (logKey, tenant, company, resourceClass, resourceType, resourceCategory) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler -  GetResourcesByTags :: tenant: %d :: company: %d :: resourceClass: %s :: resourceType: %s :: resourceCategory: %s', logKey, tenant, company, resourceClass, resourceType, resourceCategory);

        var resourceSearchTags = [
            'Tag:Resource:tenant_' + tenant,
            'Tag:Resource:company_' + company,
            'Tag:Resource:objType_Resource'
        ];
        if (resourceClass)
            resourceSearchTags.push('Tag:Resource:class_' + resourceClass);
        if (resourceType)
            resourceSearchTags.push('Tag:Resource:type' + resourceType);
        if (resourceCategory)
            resourceSearchTags.push('Tag:Resource:category_' + resourceCategory);

        redisHandler.R_SInter(logKey, resourceSearchTags).then(function (resourceSearchData) {

            if (resourceSearchData && resourceSearchData.length > 0) {

                var asyncTasks = [];
                resourceSearchData.forEach(function (searchData) {
                    asyncTasks.push(
                        function (callback) {
                            getResource(logKey, tenant, company, '', searchData).then(function (result) {
                                callback(null, result);
                            }).catch(function (err) {
                                callback(err, null);
                            })
                        }
                    );
                });

                async.parallel(async.reflectAll(asyncTasks), function (err, results) {

                    var resourceData = [];
                    results.forEach(function (result) {
                        if (result && result.value)
                            resourceData.push(result.value);
                    });
                    logger.info('LogKey: %s - ResourceHandler - GetResourcesByTags - success :: %j', logKey, resourceData);
                    deferred.resolve(resourceData);

                });

            } else {

                logger.error('LogKey: %s - ResourceHandler - GetResourcesByTags - R_SInter: %j :: No resource search data found', logKey, resourceSearchTags);
                deferred.reject('No resource status data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - GetResourcesByTags - R_SInter: %j failed', logKey, resourceSearchTags);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - GetResourcesByTags failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


var updateLastConnectedTime = function (logKey, tenant, company, resourceId, task, event, maxRejectCount) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler -  UpdateLastConnectedTime :: tenant: %d :: company: %d :: resourceId: %s :: task: %s :: event: %s :: maxRejectCount: %d', logKey, tenant, company, resourceId, task, event, maxRejectCount);

        if (event == "reserved" || event == "connected") {
            var concurrencyKey = util.format('ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, task);
            var concurrencyVersionKey = util.format('Version:ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, task);
            var concurrencyVersion = null;

            redisHandler.R_Get(logKey, concurrencyVersionKey).then(function (version) {

                logger.info('LogKey: %s - ResourceHandler - UpdateLastConnectedTime - R_Get version success: %s :: %s', logKey, concurrencyVersionKey, version);

                concurrencyVersion = version;
                return redisHandler.R_Get(logKey, concurrencyKey);

            }).then(function (concurrencyData) {

                if (concurrencyData) {

                    logger.info('LogKey: %s - ResourceHandler - UpdateLastConnectedTime - R_Get concurrency success: %s', logKey, concurrencyKey);

                    var concurrencyObj = JSON.parse(concurrencyData);
                    var date = new Date();

                    switch (event) {
                        case 'reserved':
                            concurrencyObj.MaxRejectCount = maxRejectCount;
                            concurrencyObj.LastConnectedTime = date.toISOString();
                            break;
                        case 'connected':
                            concurrencyObj.RejectCount = 0;
                            break;
                        default :
                            break;
                    }

                    redisHandler.R_VersionValidate(logKey, concurrencyVersionKey, concurrencyVersion).then(function (versionResult) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateLastConnectedTime - R_VersionValidate success: %s', logKey, versionResult);
                        redisHandler.R_Set(logKey, concurrencyKey, JSON.stringify(concurrencyObj)).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateLastConnectedTime success: %s', logKey, result);
                            deferred.resolve(result);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceHandler - UpdateLastConnectedTime failed: %s :: %s', logKey, concurrencyKey, ex);
                            deferred.reject(ex);
                        })

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateLastConnectedTime - R_VersionValidate :: failed', logKey, ex);
                        deferred.reject(ex);
                    });

                } else {

                    logger.info('LogKey: %s - ResourceHandler - UpdateLastConnectedTime - No concurrency data found: %s', logKey, concurrencyKey);
                    deferred.reject('No concurrency data found');
                }

            }).catch(function (ex) {

                logger.error('LogKey: %s - ResourceHandler - UpdateLastConnectedTime :: failed', logKey, ex);
                deferred.reject(ex);
            });
        } else {

            logger.error('LogKey: %s - ResourceHandler - UpdateLastConnectedTime :: invalid event : %s', logKey, event);
            deferred.reject('Invalid event');
        }

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateLastConnectedTime failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var updateRejectCount = function (logKey, tenant, company, resourceId, task, rejectedSession, reason) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler -  UpdateRejectCount :: tenant: %d :: company: %d :: resourceId: %s :: task: %s :: rejectedSession: %s :: reason: %s', logKey, tenant, company, resourceId, task, rejectedSession, reason);

        var concurrencyKey = util.format('ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, task);
        var concurrencyVersionKey = util.format('Version:ConcurrencyInfo:%d:%d:%d:%s', tenant, company, resourceId, task);
        var concurrencyVersion = null;

        redisHandler.R_Get(logKey, concurrencyVersionKey).then(function (version) {

            logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - R_Get version success: %s :: %s', logKey, concurrencyVersionKey, version);

            concurrencyVersion = version;
            return redisHandler.R_Get(logKey, concurrencyKey);

        }).then(function (concurrencyData) {

            if (concurrencyData) {

                logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - R_Get concurrency success: %s', logKey, concurrencyKey);

                var concurrencyObj = JSON.parse(concurrencyData);

                concurrencyObj.RejectCount = concurrencyObj.RejectCount + 1;
                concurrencyObj.LastRejectedSession = rejectedSession;
                if (concurrencyObj.RejectCount >= concurrencyObj.MaxRejectCount) {

                    logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - Reject Count Exceeded: %s', logKey, concurrencyObj.UserName);
                    concurrencyObj.IsRejectCountExceeded = true;

                    var notificationMsg = {
                        From: "ARDS",
                        Direction: "STATELESS",
                        To: cObj.UserName,
                        Message: "Reject count Exceeded!, Account suspended for Task:: " + concurrencyObj.HandlingType
                    };
                    notificationService.SendNotificationInitiate(logKey, tenant, company, "message", "", notificationMsg);
                    notificationService.SendNotificationInitiate(logKey, tenant, company, "agent_suspended", "", notificationMsg);
                }

                redisHandler.R_VersionValidate(logKey, concurrencyVersionKey, concurrencyVersion).then(function (versionResult) {

                    logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - R_VersionValidate success: %s', logKey, versionResult);
                    redisHandler.R_Set(logKey, concurrencyKey, JSON.stringify(concurrencyObj)).then(function (result) {

                        if (concurrencyObj.RejectCount >= concurrencyObj.MaxRejectCount) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - Reject Count Exceeded: %s', logKey, concurrencyObj.UserName);
                            concurrencyObj.IsRejectCountExceeded = true;

                            var notificationMsg = {
                                From: "ARDS",
                                Direction: "STATELESS",
                                To: concurrencyObj.UserName,
                                Message: "Reject count Exceeded!, Account suspended for Task:: " + concurrencyObj.HandlingType
                            };
                            notificationService.SendNotificationInitiate(logKey, tenant, company, "message", "", notificationMsg);
                            notificationService.SendNotificationInitiate(logKey, tenant, company, "agent_suspended", "", notificationMsg);
                        }

                        resourceService.AddResourceTaskRejectInfo(logKey, tenant, company, resourceId, task, reason, '', rejectedSession);

                        logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount success: %s', logKey, result);
                        deferred.resolve(result);

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateRejectCount failed: %s :: %s', logKey, concurrencyKey, ex);
                        deferred.reject(ex);
                    })

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - UpdateRejectCount - R_VersionValidate :: failed', logKey, ex);
                    deferred.reject(ex);
                });

            } else {

                logger.info('LogKey: %s - ResourceHandler - UpdateRejectCount - No concurrency data found: %s', logKey, concurrencyKey);
                deferred.reject('No concurrency data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - UpdateRejectCount :: failed', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateRejectCount failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var updateSlotStateReserved = function (logKey, tenant, company, resourceId, task, slotId, sessionId, maxReservedTime, maxAfterWorkTime, maxFreezeTime, maxRejectCount, otherInfo) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved :: tenant: %d :: company: %d :: resourceId: %d :: task: %s :: slotId: %d :: sessionId: %s :: maxReservedTime: %d :: maxAfterWorkTime: %d :: maxFreezeTime: %d :: maxRejectCount: %d :: otherInfo: %s', logKey, tenant, company, resourceId, task, slotId, sessionId, maxReservedTime, maxAfterWorkTime, maxFreezeTime, maxRejectCount, otherInfo);

        var slotDataKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotDataVersionKey = util.format('Version:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotVersion = null;

        redisHandler.R_Get(logKey, slotDataVersionKey).then(function (version) {

            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_Get version success: %s :: %s', logKey, slotDataVersionKey, version);

            slotVersion = version;
            return redisHandler.R_Get(logKey, slotDataKey);

        }).then(function (slotData) {

            if (slotData) {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_Get slot success: %s', logKey, slotDataKey);

                var slotObj = JSON.parse(slotData);

                var date = new Date();
                var lastStatusChangedTime = deepcopy(slotObj.StateChangeTime);
                var lastStatus = deepcopy(slotObj.State);
                var lastOtherInfo = deepcopy(slotObj.OtherInfo);
                slotObj.State = "Reserved";
                slotObj.StateChangeTime = date.toISOString();
                slotObj.HandlingRequest = sessionId;
                slotObj.LastReservedTime = date.toISOString();
                slotObj.OtherInfo = otherInfo;
                slotObj.MaxReservedTime = maxReservedTime;
                slotObj.MaxAfterWorkTime = maxAfterWorkTime;
                slotObj.MaxFreezeTime = maxFreezeTime;

                redisHandler.R_VersionValidate(logKey, slotDataVersionKey, slotVersion).then(function (versionResult) {

                    logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_VersionValidate success: %s', logKey, versionResult);

                    var requestHandlingTagKey = util.format('Tag:SlotInfo:handlingRequest_%s', sessionId);

                    redisHandler.R_Set(logKey, requestHandlingTagKey, slotObj.ObjKey).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_Set requestHandlingTagKey: %s :: %s', logKey, requestHandlingTagKey, result);
                        return redisHandler.R_Set(logKey, slotDataKey, JSON.stringify(slotObj));

                    }).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_Set Slot data: %s :: %s', logKey, slotDataKey, result);
                        var tagSourceKey = util.format('Tag:SlotInfo:state_%s', lastStatus);
                        var tagDestinationKey = util.format('Tag:SlotInfo:state_%s', slotObj.State);
                        return tagHandler.MoveTag(logKey, tagSourceKey, tagDestinationKey, slotObj.ObjKey);

                    }).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - Move slot Tag :: %s', logKey, result);

                        var duration = moment(slotObj.StateChangeTime).diff(moment(lastStatusChangedTime), 'seconds');
                        var postAsyncTasks = [
                            function (callback) {
                                updateLastConnectedTime(logKey, tenant, company, resourceId, task, 'reserved', maxRejectCount).then(function (result) {
                                    callback(null, result);
                                }).catch(function (ex) {
                                    callback(ex, null);
                                });
                            },
                            function (callback) {
                                resourceStatusMapper.SetResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'SlotStatus', slotObj.State, otherInfo, {
                                    SessionId: sessionId,
                                    Direction: ""
                                }).then(function () {
                                    resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'SlotStatus', lastStatus, '', lastOtherInfo, sessionId, duration).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }).catch(function (ex) {
                                    callback(ex, null);
                                });
                            }
                        ];

                        async.parallel(async.reflectAll(postAsyncTasks), function () {
                            logger.info('LogKey: %s - ResourceHandler - AddResourceStatusChangeInfo :: Success', logKey);
                        });

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved success: %s', logKey, result);
                        deferred.resolve(result);

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateReserved failed: %s :: %s', logKey, slotDataKey, ex);
                        deferred.reject(ex);
                    })

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - R_VersionValidate :: failed', logKey, ex);
                    deferred.reject(ex);
                });

            } else {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved - No slot data found: %s', logKey, slotDataKey);
                deferred.reject('No slot data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateReserved :: failed', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateReserved failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var updateSlotStateConnected = function (logKey, tenant, company, resourceId, task, slotId, sessionId, direction, otherInfo) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateReserved :: tenant: %d :: company: %d :: resourceId: %d :: task: %s :: slotId: %d :: sessionId: %s :: direction: %s :: otherInfo: %s', logKey, tenant, company, resourceId, task, slotId, sessionId, direction, otherInfo);

        var slotDataKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotDataVersionKey = util.format('Version:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotVersion = null;

        redisHandler.R_Get(logKey, slotDataVersionKey).then(function (version) {

            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_Get version success: %s :: %s', logKey, slotDataVersionKey, version);

            slotVersion = version;
            return redisHandler.R_Get(logKey, slotDataKey);

        }).then(function (slotData) {

            if (slotData) {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_Get slot success: %s', logKey, slotDataKey);

                var slotObj = JSON.parse(slotData);

                var date = new Date();
                var lastStatusChangedTime = deepcopy(slotObj.StateChangeTime);
                var lastStatus = deepcopy(slotObj.State);
                var lastOtherInfo = deepcopy(slotObj.OtherInfo);

                slotObj.State = "Connected";
                slotObj.StateChangeTime = date.toISOString();
                slotObj.HandlingRequest = sessionId;
                slotObj.OtherInfo = otherInfo;

                redisHandler.R_VersionValidate(logKey, slotDataVersionKey, slotVersion).then(function (versionResult) {

                    logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_VersionValidate success: %s', logKey, versionResult);

                    var requestHandlingTagKey = util.format('Tag:SlotInfo:handlingRequest_%s', sessionId);

                    redisHandler.R_Set(logKey, requestHandlingTagKey, slotObj.ObjKey).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_Set requestHandlingTagKey: %s :: %s', logKey, requestHandlingTagKey, result);
                        return redisHandler.R_Set(logKey, slotDataKey, JSON.stringify(slotObj));

                    }).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_Set Slot data: %s :: %s', logKey, slotDataKey, result);
                        var tagSourceKey = util.format('Tag:SlotInfo:state_%s', lastStatus);
                        var tagDestinationKey = util.format('Tag:SlotInfo:state_%s', slotObj.State);
                        return tagHandler.MoveTag(logKey, tagSourceKey, tagDestinationKey, slotObj.ObjKey);

                    }).then(function (result) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - Move slot Tag :: %s', logKey, result);

                        var duration = moment(slotObj.StateChangeTime).diff(moment(lastStatusChangedTime), 'seconds');
                        var postAsyncTasks = [
                            function (callback) {
                                updateLastConnectedTime(logKey, tenant, company, resourceId, task, 'connected', 0).then(function (result) {
                                    callback(null, result);
                                }).catch(function (ex) {
                                    callback(ex, null);
                                });
                            },
                            function (callback) {
                                if (otherInfo == "" || otherInfo == null)
                                    otherInfo = "Connected";

                                resourceStatusMapper.SetResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'SlotStatus', slotObj.State, task, {
                                    SessionId: sessionId,
                                    Direction: direction
                                }).then(function () {
                                    resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'SlotStatus', lastStatus, '', lastOtherInfo, sessionId, duration).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }).catch(function (ex) {
                                    callback(ex, null);
                                });
                            }
                        ];

                        async.parallel(async.reflectAll(postAsyncTasks), function () {
                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected :: Success', logKey);
                        });

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected success: %s', logKey, result);
                        deferred.resolve(result);

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateConnected failed: %s :: %s', logKey, slotDataKey, ex);
                        deferred.reject(ex);
                    })

                }).catch(function (ex) {

                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - R_VersionValidate :: failed', logKey, ex);
                    deferred.reject(ex);
                });

            } else {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateConnected - No slot data found: %s', logKey, slotDataKey);
                deferred.reject('No slot data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateConnected :: failed', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateConnected failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var updateSlotStateAfterWork = function (logKey, tenant, company, resourceId, task, slotId, sessionId, direction, reason, otherInfo) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork :: tenant: %d :: company: %d :: resourceId: %d :: task: %s :: slotId: %d :: sessionId: %s :: direction: %s :: reason: %s :: otherInfo: %s', logKey, tenant, company, resourceId, task, slotId, sessionId, direction, reason, otherInfo);

        var slotDataKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotDataVersionKey = util.format('Version:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotVersion = null;

        redisHandler.R_Get(logKey, slotDataVersionKey).then(function (version) {

            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - R_Get version success: %s :: %s', logKey, slotDataVersionKey, version);

            slotVersion = version;
            return redisHandler.R_Get(logKey, slotDataKey);

        }).then(function (slotData) {

            if (slotData) {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - R_Get slot success: %s', logKey, slotDataKey);

                var slotObj = JSON.parse(slotData);

                var date = new Date();
                var lastStatusChangedTime = deepcopy(slotObj.StateChangeTime);
                var lastStatus = deepcopy(slotObj.State);
                var lastOtherInfo = deepcopy(slotObj.OtherInfo);

                if (lastStatus === "Connected" || lastStatus === "Reserved") {

                    slotObj.State = "AfterWork";
                    slotObj.StateChangeTime = date.toISOString();
                    slotObj.OtherInfo = otherInfo;

                    redisHandler.R_VersionValidate(logKey, slotDataVersionKey, slotVersion).then(function (versionResult) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - R_VersionValidate success: %s', logKey, versionResult);

                        redisHandler.R_Set(logKey, slotDataKey, JSON.stringify(slotObj)).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - R_Set Slot data: %s :: %s', logKey, slotDataKey, result);

                            var duration = moment(slotObj.StateChangeTime).diff(moment(lastStatusChangedTime), 'seconds');
                            var postAsyncTasks = [
                                function (callback) {
                                    resourceStatusMapper.SetResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'SlotStatus', 'Completed', task, {
                                        SessionId: sessionId,
                                        Direction: task+direction
                                    }).then(function () {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                },
                                function (callback) {
                                    resourceStatusMapper.SetResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'SlotStatus', 'Completed', slotObj.State, {
                                        SessionId: sessionId,
                                        Direction: task+direction
                                    }).then(function () {
                                        resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'SlotStatus', lastStatus, '', lastOtherInfo, sessionId, duration).then(function (result) {
                                            callback(null, result);
                                        }).catch(function (ex) {
                                            callback(ex, null);
                                        });
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }
                            ];

                            async.parallel(async.reflectAll(postAsyncTasks), function () {
                                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork :: Success', logKey);
                            });

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork success: %s', logKey, result);
                            deferred.resolve(result);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork failed: %s :: %s', logKey, slotDataKey, ex);
                            deferred.reject(ex);
                        })

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - R_VersionValidate :: failed', logKey, ex);
                        deferred.reject(ex);
                    });
                }else{

                    logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - Cannot proceed to AfterWork while on %s state', logKey, lastStatus);
                    deferred.reject(util.format('Cannot proceed to AfterWork while on %s state', lastStatus));
                }

            } else {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork - No slot data found: %s', logKey, slotDataKey);
                deferred.reject('No slot data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork :: failed', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAfterWork failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var updateSlotStateAvailable = function (logKey, tenant, company, resourceId, task, slotId, reason, otherInfo, callingParty) {
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable :: tenant: %d :: company: %d :: resourceId: %d :: task: %s :: slotId: %d :: reason: %s :: callingParty: %s :: otherInfo: %s', logKey, tenant, company, resourceId, task, slotId, reason, callingParty, otherInfo);

        var slotDataKey = util.format('CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotDataVersionKey = util.format('Version:CSlotInfo:%d:%d:%d:%s:%d', tenant, company, resourceId, task, slotId);
        var slotVersion = null;

        redisHandler.R_Get(logKey, slotDataVersionKey).then(function (version) {

            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_Get version success: %s :: %s', logKey, slotDataVersionKey, version);

            slotVersion = version;
            return redisHandler.R_Get(logKey, slotDataKey);

        }).then(function (slotData) {

            if (slotData) {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_Get slot success: %s', logKey, slotDataKey);

                var slotObj = JSON.parse(slotData);

                var date = new Date();
                var lastStatusChangedTime = deepcopy(slotObj.StateChangeTime);
                var lastStatus = deepcopy(slotObj.State);
                var lastOtherInfo = deepcopy(slotObj.OtherInfo);
                var handledRequest = deepcopy(slotObj.HandlingRequest);

                if (callingParty === "Completed" && slotObj.State === "AfterWork" && slotObj.FreezeAfterWorkTime === true) {

                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - Reject available request:: Completed', logKey);
                    deferred.reject('Reject available request:: Completed');
                } else if (callingParty === "Completed" && slotObj.State === "Reserved") {

                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - Reject available request:: Reserved', logKey);
                    deferred.reject('Reject available request:: Reserved');
                } else if (callingParty === "Completed" && slotObj.State === "Connected") {

                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - Reject available request:: Connected', logKey);
                    deferred.reject('Reject available request:: Connected');
                } else if (callingParty === "endFreeze" && slotObj.State != "AfterWork") {
                    logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - Reject available request:: EndFreeze', logKey);
                    deferred.reject('Reject available request:: EndFreeze');
                } else {

                    slotObj.State = "Available";
                    slotObj.StateChangeTime = date.toISOString();
                    slotObj.HandlingRequest = "";
                    slotObj.FreezeAfterWorkTime = false;
                    slotObj.OtherInfo = "";

                    redisHandler.R_VersionValidate(logKey, slotDataVersionKey, slotVersion).then(function (versionResult) {

                        logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_VersionValidate success: %s', logKey, versionResult);

                        var requestHandlingTagKey = util.format('Tag:SlotInfo:handlingRequest_%s', handledRequest);

                        redisHandler.R_Del(logKey, requestHandlingTagKey).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_Del requestHandlingTagKey: %s :: %s', logKey, requestHandlingTagKey, result);
                            return redisHandler.R_Set(logKey, slotDataKey, JSON.stringify(slotObj));

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_Set Slot data: %s :: %s', logKey, slotDataKey, result);
                            var tagSourceKey = util.format('Tag:SlotInfo:state_%s', lastStatus);
                            var tagDestinationKey = util.format('Tag:SlotInfo:state_%s', slotObj.State);
                            return tagHandler.MoveTag(logKey, tagSourceKey, tagDestinationKey, slotObj.ObjKey);

                        }).then(function (result) {

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - Move slot Tag :: %s', logKey, result);

                            if (slotObj.FreezeAfterWorkTime) {
                                try {
                                    scheduleWorkerService.endFreeze(company, tenant, resourceId, logKey);
                                } catch (ex) {
                                    console.log('scheduleWorkerHandler.endFreeze Error :: ' + ex);
                                }
                            }

                            var duration = moment(slotObj.StateChangeTime).diff(moment(lastStatusChangedTime), 'seconds');
                            var postAsyncTasks = [
                                function (callback) {
                                    updateLastConnectedTime(logKey, tenant, company, resourceId, task, 'connected', 0).then(function (result) {
                                        callback(null, result);
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                },
                                function (callback) {
                                    if (otherInfo == "" || otherInfo == null)
                                        otherInfo = "Connected";

                                    resourceStatusMapper.SetResourceStatusChangeInfo(logKey, tenant, company, resourceId, 'SlotStatus', slotObj.State, otherInfo, {
                                        SessionId: handledRequest,
                                        Direction: ""
                                    }).then(function () {
                                        resourceService.AddResourceStatusDurationInfo(logKey, tenant, company, resourceId, 'SlotStatus', lastStatus, '', lastOtherInfo, handledRequest, duration).then(function (result) {
                                            callback(null, result);
                                        }).catch(function (ex) {
                                            callback(ex, null);
                                        });
                                    }).catch(function (ex) {
                                        callback(ex, null);
                                    });
                                }
                            ];

                            async.parallel(async.reflectAll(postAsyncTasks), function () {
                                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable :: Success', logKey);
                            });

                            logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable success: %s', logKey, result);
                            deferred.resolve(result);

                        }).catch(function (ex) {

                            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable failed: %s :: %s', logKey, slotDataKey, ex);
                            deferred.reject(ex);
                        })

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - R_VersionValidate :: failed', logKey, ex);
                        deferred.reject(ex);
                    });
                }

            } else {

                logger.info('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable - No slot data found: %s', logKey, slotDataKey);
                deferred.reject('No slot data found');
            }

        }).catch(function (ex) {

            logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable :: failed', logKey, ex);
            deferred.reject(ex);
        });

    } catch (ex) {

        logger.error('LogKey: %s - ResourceHandler - UpdateSlotStateAvailable failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.RemoveResource = removeResource;
module.exports.AddResource = addResource;
module.exports.ShareResource = shareResource;
module.exports.RemoveShareResource = removeShareResource;
module.exports.SetResourceAttributes = setResourceAttributes;
module.exports.GetResource = getResource;
module.exports.GetResourceStatus = getResourceStatus;
module.exports.GetResourcesByTags = getResourcesByTags;