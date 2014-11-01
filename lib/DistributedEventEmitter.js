
var EventEmitter = require('events').EventEmitter;
var util = require('util');

// base class
var DistributedEventEmitter = function(opts) {
    if (!opts) opts = {};
    this.topic = opts.topic || 'distributed-events';
    this.id = opts.id || require('crypto').randomBytes(20).toString('hex');
};
util.inherits(DistributedEventEmitter, EventEmitter);


module.exports = DistributedEventEmitter;