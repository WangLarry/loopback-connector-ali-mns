// Copyright IBM Corp. 2013,2016. All Rights Reserved.
// Node module: loopback-connector-mqlight
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
var AliMNS = require("ali-mns");
var debug = require('debug')('loopback:connector:mq-ali-mns');
var _ = require('lodash');
module.exports = MQResource;

// This module contains the Model specific logic for the MQLight
// connector.
function MQResource(modelName) {
    if (debug.enabled) {
        debug('Creating Model: %s', modelName);
    }

    if (!this instanceof MQResource) {
        return new MQResource(modelName);
    }
};

function buildCallback(p, cb) {
    if (!cb) return p;
    return p.then(function success(obj) {
        cb(null, obj);
    }, function error(err) {
        cb(err);
    });
}
//缓存 队列
MQResource.prototype.makeQueue = function(name) {
        if (this._queues[name]) {
            return this._queues[name]
        }
        var mq = new AliMNS.MQ(name, account, this.settings.region);
        this._queues[name] = mq;
        return this._queues[name];
    }
    //发送消息
MQResource.prototype.createMessage = function(data, cb) {
    var self = this;
    var topic = data.topic;
    var message = JSON.stringify(data.message);
    var priority = data.priority;
    var delaySeconds = data.delaySeconds;
    var mq = self.makeQueue(topic);

    if (debug.enabled) {
        debug('Creating message: message=%s', message);
        debug('Creating message: topic=%s', topic);
        debug('Creating message: priority=%s', priority);
        debug('Creating message: delaySeconds=%s', delaySeconds);
    }

    var p = mq.sendP(message, priority, delaySeconds);
    var wrap = new Promise(function(resolve, reject) {
        p.then(function success(obj) {
            if (data != 204 || data != 201) {
                return reject(data);
            }
            return resolve(data);
        }, function error(err) {
            if (err && err.code != 'QueueAlreadyExist') {
                return reject(err);
            }
            return resolve(204);
        })
    });
    return buildCallback(wrap, cb);
};
//更新消息的可见时间
MQResource.prototype.changeMessageVisibility = function(data, cb) {

    var self = this;
    var topic = data.topic;
    var receiptHandle = data.receiptHandle;
    var visibilityTimeout = data.visibilityTimeout;
    var mq = self.makeQueue(topic);

    if (debug.enabled) {
        debug('changeMessageVisibility message: topic = %s', data.topic);
        debug('changeMessageVisibility message: receiptHandle = %s', data.receiptHandle);
    }
    var p = mq.reserveP(receiptHandle, visibilityTimeout);
    return buildCallback(p, cb);
};
//删除消息
MQResource.prototype.deleteMessage = function(data, cb) {

    var self = this;
    var topic = data.topic;
    var receiptHandle = data.receiptHandle;
    var mq = self.makeQueue(topic);

    if (debug.enabled) {
        debug('delete message: topic = %j', topic);
        debug('delete message: receiptHandle = %j', receiptHandle);
    }

    var p = mq.deleteP(receiptHandle);
    return buildCallback(p, cb);
};
//注册消息通知
MQResource.prototype.notifyMessage = function(data, cb) {
    var self = this;
    var topic = data.topic;

    if (debug.enabled) {
        debug('register notify message from %s', topic);
    }

    var mq = self.makeQueue(topic);
    var dealres = function(err, message) {
        if (debug.enabled) {
            debug('notify message from %s', topic);
            debug('notify message content %s', JSON.stringify(message));
        }
        cb(err, message);
    }
    mq.notifyRecv(dealres, self.settings.waitSeconds);
};
//创建队列
MQResource.prototype.createQueue = function(data, cb) {
        var self = this;
        var topic = data.topic;
        if (debug.enabled) {
            debug('create Queue %s', JSON.stringify(data));
        }
        var prop = _.pick(data, ['DelaySeconds', 'MaximumMessageSize', 'MessageRetentionPeriod', 'VisibilityTimeout', 'PollingWaitSeconds']);
        var p = self.mns.createP(topic, prop);
        return buildCallback(p, cb);
    }
    //修改队列属性
MQResource.prototype.updateQueue = function(data, cb) {
        var self = this;
        var topic = data.topic;
        if (debug.enabled) {
            debug('update Queue %s', JSON.stringify(data));
        }
        var mq = self.makeQueue(topic);
        var prop = _.pick(data, ['DelaySeconds', 'MaximumMessageSize', 'MessageRetentionPeriod', 'VisibilityTimeout', 'PollingWaitSeconds']);
        var p = mq.setAttrsP(prop);
        return buildCallback(p, cb);
    }
    //查询队列 
MQResource.prototype.listQueue = function(data, cb) {
    var self = this;
    if (debug.enabled) {
        debug('list Queue %s', JSON.stringify(data));
    }
    var prefix = data.prefix;
    var pageSize = data.pageSize;
    var pageMarker = data.pageMarker;
    var p = self.mns.listP(prefix, pageSize, pageMarker);
    return buildCallback(p, cb);
}
