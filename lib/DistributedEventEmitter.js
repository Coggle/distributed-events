
var EventEmitter = require('events').EventEmitter;
var util = require('util');

// base class
var DistributedEventEmitter = function(topic) {
  this.topic = topic || 'distributed-events';
};
util.inherits(DistributedEventEmitter, EventEmitter);


module.exports = DistributedEventEmitter;