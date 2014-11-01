
var DistributedEventEmitter = require('./DistributedEventEmitter');
var AWS = require('aws-sdk');
var util = require('util');
var _ = require('underscore');

var SNSEventEmitter = function(opts, callback){
    DistributedEventEmitter.call(this, opts);

    AWS.config.update({
        accessKeyId: opts.accessKeyId,
        secretAccessKey: opts.secretAccessKey,
        region: opts.region
    });

    this.SNS = new AWS.SNS({apiVersion: '2010-03-31'});
    this.SQS = new AWS.SQS({apiVersion: '2012-11-05'});

    var self = this;
    self.getTopic(self.topic, function(err, topicArn){
        if (err) return callback(err);
        self.getQueue(function(err, queueArn) {
            if (err) return callback(err);
            self.subscribe(topicArn, queueArn, function(err, subscriptionArn){
                if (err) return callback(err);
                self.subscriptionArn = subscriptionArn;
                callback(err, self);

                function runPoll(err){
                    if (err) DistributedEventEmitter.call(self, 'poll_error', err);
                    self.poll(runPoll);
                }
                runPoll();
            });
        });
    });
};
util.inherits(SNSEventEmitter, DistributedEventEmitter);

SNSEventEmitter.prototype.poll = function(callback){
    var self = this;
    self.getQueue(function(err, queueArn, queueUrl){
        if (err) return callback(err);
        self.SQS.receiveMessage({
            QueueUrl: self.queueUrl,
            MaxNumberOfMessages: 10,
        }, function(err, data) {
            if (err) return callback(err);
            var handles = null;
            if (data.Messages) {            
                // fire events
                handles = data.Messages.map(function(msg) {
                    var content = JSON.parse(msg.Body);
                    var args = JSON.parse(content.Message);
                    DistributedEventEmitter.prototype.emit.apply(self, args);
                    return msg.ReceiptHandle;
                });
            }
            // then delete read messages
            self.ack(handles, function(err) {
                if (err) return callback(err);
                self.poll(callback);
            });
        });
    });
};

SNSEventEmitter.prototype.ack = function(handles, callback){
    var self = this;

    if (!handles) return callback();

    var entries = handles.map(function(handle, i) {
        return {
            Id: i+'',
            ReceiptHandle: handle
        };
    });

    self.getQueue(function(err, queueArn, queueUrl){
        if (err) return callback(err);
        self.SQS.deleteMessageBatch({
            QueueUrl: queueUrl,
            Entries: entries
        }, callback);
    });
};

SNSEventEmitter.prototype.getQueue = function(callback){
    var self = this;

    if (self.queueUrl && self.queueArn) {
        return process.nextTick(function() {
            callback(false, self.queueArn, self.queueUrl);
        });
    }

    self.SQS.createQueue({
        "QueueName": self.id,
        "Attributes":{
            "MessageRetentionPeriod": "60",
            "ReceiveMessageWaitTimeSeconds": "20"
        }
    }, function(err, data){
        if (err) return callback(err);
        self.SQS.getQueueAttributes({
            QueueUrl: data.QueueUrl,
            AttributeNames: ["QueueArn"]
        }, function(err, attrs){
            if (err) return callback(err);
            self.queueArn = attrs.Attributes.QueueArn;
            self.queueUrl = data.QueueUrl;
            self.setPermissions(self.id, self.topicArn, self.queueArn, self.queueUrl, function(err) {
                if (err) return callback(err);
                callback(false, attrs.Attributes.QueueArn, data.QueueUrl);
            });
        });
    });
};


SNSEventEmitter.prototype.setPermissions = function(id, topicArn, queueArn, queueUrl, callback){
    var self = this;
    var policy = {
        "Version":"2012-10-17",
        "Statement":[{
            "Sid":id,
            "Effect":"Allow",
            "Principal":"*",
            "Action":"sqs:SendMessage",
            "Resource":queueArn,
            "Condition":{
                "ArnEquals":{
                    "aws:SourceArn":topicArn
                }
            }
        }]
    };

    self.SQS.setQueueAttributes({
        QueueUrl: queueUrl, 
        Attributes: {
            Policy:JSON.stringify(policy)
        }
    }, callback);
};


SNSEventEmitter.prototype.getTopic = function(topic, callback){
    if (this.topicArn) {
        return process.nextTick(function() {
            callback(false, this.topicArn);
        }.bind(this));
    }

    this.SNS.createTopic({
        'Name': topic
    }, function (err, result) {
        if (err) return callback(err);
        callback(false, result.TopicArn);
    }.bind(this));
};

SNSEventEmitter.prototype.subscribe = function(topicArn, queueArn, callback){
    this.SNS.subscribe({
        TopicArn: topicArn,
        Protocol: "sqs",
        Endpoint: queueArn,
    }, function(err, data) {
        if (err) return callback(err);
        else callback(false, data.SubscriptionArn);
    });
};

SNSEventEmitter.prototype.emit = function(event){
    var self = this;
    var args = _.values(arguments);

    this.getTopic(self.topic, function(err, topicArn) {
        var message = {
            TopicArn: topicArn,
            Subject: event,
            Message: JSON.stringify(args)
        };
        self.SNS.publish(message, function(err, result){
            if (err) DistributedEventEmitter.prototype.emit.call(self, 'error', err);
        });
    });
};

module.exports = SNSEventEmitter;