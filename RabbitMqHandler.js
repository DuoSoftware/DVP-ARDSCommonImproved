/**
 * Created by Heshan.i on 10/24/2017.
 */

var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var amqp = require('amqp');
var config = require('config');
var util = require('util');
var q = require('q');


var amqpIPs = [];
if(config.RabbitMQ.ip) {
    amqpIPs = config.RabbitMQ.ip.split(",");
}


var queueConnection = amqp.createConnection({
    host: amqpIPs,
    port: config.RabbitMQ.port,
    login: config.RabbitMQ.user,
    password: config.RabbitMQ.password,
    vhost: config.RabbitMQ.vhost,
    noDelay: true,
    heartbeat:10
}, {
    reconnect: true,
    reconnectBackoffStrategy: 'linear',
    reconnectExponentialLimit: 120000,
    reconnectBackoffTime: 1000
});

queueConnection.on('ready', function () {

    logger.info("Connection with the queue is OK");

});

var Publish = function(logKey, messageType, sendObj){
    var deferred = q.defer();

    try {
        logger.info('LogKey: %s - RabbitMqHandler - Publish :: messageType: %s :: sendObj: %j', logKey, messageType, sendObj);

        queueConnection.publish(messageType, sendObj, {
            contentType: 'application/json'
        });
        logger.info('LogKey: %s - RabbitMqHandler - Publish :: success', logKey);

    }catch(exp){

        logger.error('LogKey: %s - RabbitMqHandler - Publish :: failed', logKey);
    }

    return deferred.promise;
};


module.exports.Publish = Publish;