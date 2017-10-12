/**
 * Created by Heshan.i on 10/10/2017.
 */

var q = require('q');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var redisHandler = require('./RedisHandler');
var util = require('util');


var setTags = function (logKey, tagPrefix, tags, tagValue) {
    var deferred = q.defer();

    try{
        logger.info('LogKey: %s - TagHandler - SetTags :: tagPrefix: %s :: tags: %j :: tagValue: %s', logKey, tagPrefix, tags, tagValue);

        var pipeCommands = [];
        tags.forEach(function (tag) {
            pipeCommands.push([
                'sadd',
                util.format('%s:%s', tagPrefix, tag),
                tagValue
            ]);

            pipeCommands.push([
                'sadd',
                util.format('%s:%s', 'TagReference', tagValue),
                util.format('%s:%s', tagPrefix, tag)
            ]);
        });

        if(pipeCommands.length >0){

            redisHandler.R_Pipeline(logKey, pipeCommands).then(function (result) {

                logger.info('LogKey: %s - TagHandler - SetTags :: pipe commands execution success', logKey);
                deferred.resolve('Tag execution finished');
            }).catch(function (ex) {
                logger.error('LogKey: %s - TagHandler - SetTags :: Pipe commands execution failed', logKey);
                deferred.reject('Pipe commands execution failed');
            });
        }else{

            logger.info('LogKey: %s - TagHandler - SetTags :: No pipe commands to proceed', logKey);
            deferred.resolve('Tag execution finished');
        }

    }catch(ex){

        logger.error('LogKey: %s - TagHandler - SetTags failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};

var removeTags = function (logKey, tagValue) {
    var deferred = q.defer();
    var tagReference = util.format('%s:%s', 'TagReference', tagValue);
    try{
        logger.info('LogKey: %s - TagHandler - RemoveTags :: tagReference: %s', logKey, tagReference);

        redisHandler.R_SMembers(tagReference).then(function (tagRefValues) {

            if(tagRefValues){

                var pipeCommands = [];
                tagRefValues.forEach(function (tagRefValue) {
                    pipeCommands.push([
                        'srem',
                        tagRefValue,
                        tagReference
                    ]);
                });

                if(pipeCommands.length >0){

                    redisHandler.R_Pipeline(logKey, pipeCommands).then(function (result) {

                        logger.info('LogKey: %s - TagHandler - RemoveTags :: pipe commands execution success', logKey);
                        return redisHandler.R_Del(tagReference);

                    }).then(function (result) {

                        logger.info('LogKey: %s - TagHandler - RemoveTags :: Remove Tag reference success', logKey);
                        deferred.resolve('Tag execution finished');

                    }).catch(function (ex) {

                        logger.error('LogKey: %s - TagHandler - RemoveTags :: Pipe commands execution failed', logKey);
                        deferred.reject('Pipe commands execution failed');

                    });
                }else{

                    logger.info('LogKey: %s - TagHandler - RemoveTags :: No pipe commands to proceed', logKey);
                    deferred.resolve('Tag execution finished');
                }

            }else{

                deferred.resolve('No tag references found');
            }
            
        }).catch(function (ex) {

            logger.info('LogKey: %s - TagHandler - RemoveTags :: Get tag references failed', logKey);
            deferred.reject('Get Tag references failed');
        });

    }catch(ex){

        logger.error('LogKey: %s - TagHandler - RemoveTag failed :: %s', logKey, ex);
        deferred.reject(ex);
    }

    return deferred.promise;
};


module.exports.SetTags = setTags;
module.exports.RemoveTags = removeTags;