// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-mqlight
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';
var g = require('./globalize');
var semver = require('semver');
var AliMNS = require("ali-mns");
var co = require("co");
var Promise = require("bluebird");
var _ = require('lodash');
var debug = require('debug')('loopback:connector:mq-ali-mns');

var runningOnNode10 = semver.satisfies(process.version, '^0.10.x');
var EventEmitter = runningOnNode10 ?
    require('events').EventEmitter :
    require('events');

// Require util so we can inherit the EventEmitter class
var util = require('util');

// Require MQResource model module
var MQResource = require('./mq-model.js');

/**
 * Initialize the  connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {

    if (!dataSource.settings) {
        return callback(new Error(g.f('Invalid settings object')));
    }

    var settings = dataSource.settings;
    var connector = new MQConnector({ settings: settings });

    dataSource.connector = connector;
    dataSource.connector.dataSource = dataSource;

    connector.connect(callback);
};

function MQConnector(config) {
    this.settings = config.settings;

    if (debug.enabled) {
        debug('Settings: %j', config.settings);
    }

    EventEmitter.call(this);

    this._models = {};
    // this._resources = {};
    //记录所有用到的队列 销毁资源的时候使用
    this._queues = {};
    

    this.DataAccessObject = function() {};
};

util.inherits(MQConnector, EventEmitter);

MQConnector.prototype.setupDataAccessObject = function() {
    var self = this;
    //对消息的增删改查
    this.DataAccessObject.createMessage = MQResource.prototype.createMessage.bind(self);
    this.DataAccessObject.changeMessageVisibility = MQResource.prototype.changeMessageVisibility.bind(self);
    this.DataAccessObject.deleteMessage = MQResource.prototype.deleteMessage.bind(self);
    this.DataAccessObject.notifyMessage = MQResource.prototype.notifyMessage.bind(self);
    //对队列的增改查
    this.DataAccessObject.createQueue = MQResource.prototype.createQueue.bind(self);
    this.DataAccessObject.updateQueue = MQResource.prototype.updateQueue.bind(self);
    this.DataAccessObject.listQueue = MQResource.prototype.listQueue.bind(self); 

    this.dataSource.DataAccessObject = this.DataAccessObject;
    for (var model in this._models) {
        if (debug.enabled) {
            debug('Mixing methods into : %s', model);
        }
        this.dataSource.mixin(this._models[model].model);
    }
    return this.DataAccessObject;
};

MQConnector.prototype.connect = function(cb) {
    var self = this;

    if (self.account) {
        process.nextTick(function() {
            if (cb) cb(null, self.account);
        });
        return;
    }

    if (debug.enabled) {
        debug('Connecting to ali mns service: %s', self.settings);
    }

    self.account = new AliMNS.Account(self.settings.accountID, self.settings.accessKeyID, self.settings.accessKeySecret);
    self.mns = new AliMNS.MNS(self.account, self.settings.region);
    self.account.connector = self;
    self.setupDataAccessObject();

    self.initQueue(function(err,data){
      if(err) {
        console.error(err);
        console.error("初始化队列失败");
        process.exit(1);
      }
      self.dataSource.connected = true;
      self.dataSource.emit('connected');
      self.dataSource.emit('started');
      cb(err,data)
    });
   
};
MQConnector.prototype.initQueue = function(cb){
  var self = this;
  co(function *(){
    if(!self.settings.initQueue||!self.settings.initQueue.settings||!self.settings.initQueue.queueList) {
      return;
    }
    var qlist = self.settings.initQueue.queueList;
    var qsetting = self.settings.initQueue.settings; 
    for(var i=0;i<qlist.length;i++) {
      var qname = qlist[i];
      var data = {topic:qname};
      _.extend(data,qsetting);
      var createQueue = Promise.promisify(self.DataAccessObject.createQueue, {context: self.DataAccessObject});
      var updateQueue = Promise.promisify(self.DataAccessObject.updateQueue, {context: self.DataAccessObject});
      //创建队列
      yield createQueue(data); 
      //更新队列属性
      yield updateQueue(data);
    }
  }).then(function (value) {
    cb(null,value);
  }, function (err) {
    cb(err);
  }); 
}
MQConnector.prototype.disconnect = function(cb) {
    var self = this; 
    for(var k in self._queues) {
      self._queues[k].notifyStopP().then(console.log, console.error);
    }
    self.dataSource.connected = false;
    self.dataSource.emit('disconnected');
};

MQConnector.prototype.define = function defineModel(definition) {
    var m = definition.model.modelName;
    this._models[m] = definition; 
};
