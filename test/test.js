var SNSEventEmitter = require('../lib/SNSEventEmitter');
var AWS = require('aws-sdk');

new SNSEventEmitter({ 
        topic: 'testing',
        accessKeyId:'XXXX',
        secretAccessKey: 'XXXX',
        region: 'eu-west-1'
    }, function(err, emitter){

    console.log(err, emitter);

    setInterval(function() {
        emitter.emit('thing', {hello:1});
    }, 1000);

    emitter.on('thing', console.log);
    emitter.on('poll_error', console.log);
    emitter.on('error', console.log);

});


