'use strict';


var BBPromise = require('bluebird');
var preq = require('preq');
var sUtil = require('../lib/util');
var _ = require('lodash');

const kafkaUtils = require('../lib/kafka-util');
const sse   = require('sse');


// shortcut
var HTTPError = sUtil.HTTPError;


/**
 * The main router object
 */
var router = sUtil.router();

/**
 * The main application object reported when this module is require()d
 */
var app;


router.get('/:topics', function(req, res) {
    // This object maps topic/partition to an object
    // suitable for passing to kafkaConsumer.assign.
    // We need to keep it as an object so we can keep track of
    // the latest offset sent to the client for a given topic partition.
    let latestOffsetsMap    = {};

    // The KafkaConsumer instance for this request.
    let kafkaConsumer       = undefined;

    // The sseClient instance for this request.
    let sseClient           = undefined;


    // Default kafkaConfigs to use if not provided in kafkaConfig.
    const defaultKafkaConfig = {
        'metadata.broker.list': 'localhost:9092',
        'client.id': `eventstreams-${req.headers['x-request-id']}`
    };

    // These configs MUST be set for an eventstreams KafkaConsumer.
    // The are not overridable.
    // We want to avoid making Kafka manage consumer info
    // for http clients.
    //   A. no offset commits
    //   B. no consumer group management/balancing.
    // A. is achieved simply by setting enable.auto.commit: false.
    // B. is more complicated. Until
    // https://github.com/edenhill/librdkafka/issues/593 is resolved,
    // there is no way to 100% keep Kafka from managing clients.  So,
    // we fake it by using the socket name, which will be unique
    // for each socket instance.  Since we use assign() instead of
    // subscribe(), at least we don't have to deal with any rebalance
    // callbacks.
    const mandatoryKafkaConfig = {
        'enable.auto.commit': false,
        'group.id': `eventstreams-${req.headers['x-request-id']}`
    };

    // Merge provided app.conf.kafka config default configs, and mandatory over all
    const kafkaConfig = Object.assign(
        defaultKafkaConfig,
        app.conf.kafka,
        mandatoryKafkaConfig
    )

    // Create and connect the promisified KafkaConsumer
    kafkaUtils.createKafkaConsumerAsync(kafkaConfig)

    // Set the kafkaConsumer for future then blocks
    // and close the consumer when the client disconnects;
    .then(consumer => {
        kafkaConsumer = consumer;
        req.on('close', function() {
            req.logger.log('info/eventstreams', `Closing KafkaConsumer ${kafkaConfig['client.id']}`);
            // TODO fix disconnect node-rdkafka bug
            kafkaConsumer.disconnect();
        });
    })


    // Build Kafka assignments from last-event-id or topics URI parameter.
    // last-event-id takes precidence;  topics param will be ignored
    // if last-event-id header is set.
    .then(() => {
        let assignments = undefined;

        // Build assignments from last-event-id header if it is set.
        if ('last-event-id' in req.headers) {
            try {
                assignments = JSON.parse(req.headers['last-event-id']);
            }
            catch (e) {
                throw new HTTPError({
                    status: 400,
                    type: 'invalid_last_event_id',
                    title: 'Invalid last-event-id',
                    detail: 'last-event-id header could not be parsed as JSON: ' + e.message,
                    last_event_id: req.headers['last-event-id'],
                });
            }
        }

        // Otherwise build assignemnts from topics param, starting at latest.
        else {
            assignments = kafkaUtils.buildAssignments(
                kafkaConsumer._metadata.topics,
                req.params.topics.split(',')
            );
        }

        // Validate the assignments we got from the client.
        try {
            kafkaUtils.validateAssignments(assignments);
        }
        catch (e) {
            throw new HTTPError({
                status: 400,
                type: 'invalid_assignments',
                title: 'Invalid Topic Partition Offset Assignments',
                detail: 'Assignments via topics or last-event-id is invalid: ' + e.message,
                assignments: assignments,
            });
        }

        return assignments;
    })
    // Assign to kafka consumer and init sseClient
    .then((assignments) => {

        // initialize the latestOffsetsMap with assignments.
        // The client will be sent latest offsets as assignments
        // as the SSE id for each message.
        assignments.forEach((a) => {
            latestOffsetsMap[`${a.topic}/${a.partition}`] = a;
        });

        // Assign the kafkaConsumer to consume at provided assignments.
        req.logger.log('info/eventstreams', { message: 'Assigning KafkaConsumer', assignments: assignments });
        kafkaConsumer.assign(assignments);
    })
    // Initialize the sseClient and return a function that uses the connected
    // sse client to send events.
    .then(() => {
        // ini
        const sseClient = new sse.Client(req, res);
        sseClient.initialize();

        /**
         * Handles JSON stringifying message and id, and then
         * sends an event to sseClient;
         */
        function sseSend(event, message, id) {
            if (!_.isString(message)) {
                message = JSON.stringify(message);
            }
            if (!_.isString(id)) {
                id = JSON.stringify(id);
            }

            return sseClient.send(event, message, id);
        }

        return sseSend;
    })

    // Then create a callback function that sends errors and messages to
    // the connected sseClient.  This callback function will be called
    // by the kafka consume loop for every consumed message or error.
    .then(sseSend => {
        function sseSendCb(error, message) {
            if (error) {
                req.logger.log('error/eventstreams', error);
                return sseSend('error', error.message, _.values(latestOffsetsMap));
            }
            else {
                // TODO: this forces us to make deserializer have the message contain a _kafka
                // property.  How can we extract the offset information transparently here???

                // Add this message's id to latestOffsetsMap object
                latestOffsetsMap[`${message._kafka.topic}/${message._kafka.partition}`] = message._kafka;
                return sseSend('message', message, _.values(latestOffsetsMap));
            }
        };

        return sseSendCb;
    })

    // Then enter the kafka consume loop, calling the function that sends messages
    // and errors to SSE clients for every consumed message.
    .then(sseSendCb => {
        // TODO: pass in configurable deserializer and matcher functions?
        return kafkaUtils.consumeLoop(kafkaConsumer, undefined, undefined, sseSendCb);
    });
});


module.exports = function(appObj) {
    app = appObj;

    return {
        path: '/v1',
        api_version: 1,
        skip_domain: true,
        router: router
    };
};

