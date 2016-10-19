'use strict';

/**
 * Collection of utility functions for working with node-rdkafka.
 */

//  TODO figure out how to connect request logger to logging here.

const objectutils         = require('./object-util');
const _                   = require('lodash');
const kafka               = require('node-rdkafka');
const P                   = require('bluebird');

/**
 * Returns a Promise of a promisified and connected rdkafka.KafkaConsumer.
 *
 * @param  {Object} kafkaConfig
 * @return {Promise<KafkaConsumer>} Promisified KafkaConsumer
 */
function createKafkaConsumerAsync(kafkaConfig) {
    let topicConfig = {};
    if ('default_topic_config' in kafkaConfig) {
        topicConfig = kafkaConfig.default_topic_config;
        delete kafkaConfig.default_topic_config;
    }

    const consumer = P.promisifyAll(
        new kafka.KafkaConsumer(kafkaConfig, topicConfig)
    );

    return consumer.connectAsync(undefined)
    .then((metadata) => {
        return consumer;
    });
}


/**
 * Return the intersection of existent topics and allowedTopics,
 * or just all existent topics if allowedTopics is undefined.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  allowedTopics topics allowed to be consumed.
 * @return {Array}  available topics
 */
function getAvailableTopics(topicsInfo, allowedTopics) {
    const existentTopics = topicsInfo.map(
        e => e.name
    )
    .filter(t => t !== '__consumer_offsets');

    if (allowedTopics) {
        return existentTopics.filter(t => _.includes(allowedTopics, t));
    }
    else {
        return existentTopics;
    }
}


/**
 * Checks assignments and makes sure it is an Array of plain objects containing
 * topic, partition, and offset properties.  If not this will throw an Error,
 * Otherwise returns true.
 *
 * @param  {Array} assignments
 * @throws {Error}
 * @return {Boolean}
 */
function validateAssignments(assignments) {
    // We will only throw this Error if assignments doesn't validate.
    // We pre-create it here just to DRY.
    let assignmentError = new Error(
        'Must provide an array of objects with topic, partition and offset.'
    );
    assignmentError.assignments = assignments;

    if (!_.isArray(assignments) || _.isEmpty(assignments)) {
        throw assignmentError;
    }
    else if (!assignments.every((a) => (
            _.isPlainObject(a)          &&
            _.isString(a.topic)         &&
            _.isInteger(a.partition)    &&
            _.isInteger(a.offset)
    ))) {
        throw assignmentError;
    }

    return true;
}


/**
 * Given an Array of topics, this will return an array of
 * [{topic: t1, partition: 0, offset: -1}, ...]
 * for each topic-partition.  This is useful for manually passing
 * an to KafkaConsumer.assign, without actually subscribing
 * a consumer group with Kafka.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  topics we want to build partition assignments for.
 * @return {Array}  TopicPartition assignments starting at latest offset.
 */
function buildAssignments(topicsInfo, topics) {
    // Flatten results
    return _.flatten(
        // Find the topic metadata we want
        topicsInfo.filter(t => _.includes(topics, t.name))
        // Map them into topic, partition, offset: -1 (latest) assignment.
        .map(t => {
            return t.partitions.map(p => {
                return { topic: t.name, partition: p.id, offset: -1 };
            });
        })
    );
}


/**
 * Parses kafkaMessage.message as a JSON string and then
 * augments the object with kafka message metadata.
 * in the _kafka sub object.
 *
 * @param  {KafkaMesssage} kafkaMessage
 * @return {Object}
 *
 */
function deserializeKafkaMessage(kafkaMessage) {
    let message = objectutils.factory(kafkaMessage.message);

    message._kafka = {
        topic:     kafkaMessage.topic,
        partition: kafkaMessage.partition,
        offset:    kafkaMessage.offset,
        key:       kafkaMessage.key
    };
    return message;
}


/**
 * Consumes messages from Kafka until we find one that parses and matches
 * via the matcher function, and then returns a Promise of that message.
 *
 * @return {Promise<Object>} Promise of first matched messaage
 */
function consume(kafkaConsumer, deserializer, matcher) {
    deserializer    = deserializer || deserializeKafkaMessage;
    matcher         = matcher || undefined;

    // Consume a message from Kafka
    return kafkaConsumer.consumeAsync()

    // Deserialize the consumed message if deserializer function is provided.
    .then((kafkaMessage) => {
        if (deserializer) {
            return deserializer(kafkaMessage);
        }
        else {
            return kafkaMessage;
        }
    })
    // Then attempt to match this message if matcher function is defined
    .then((message) => {
        if (matcher) {
            if (matcher(message)) {
                return message;
            }
            else {
                return false;
            }
        }
        else {
            return message;
        }
    })
    // Catch Kafka errors, log and re-throw real errors,
    // ignore harmless ones.
    .catch({ origin: 'kafka' }, (e) => {
        // Ignore innoculous Kafka errors.
        switch (e.code) {
            case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
            case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                // console.log(
                //     { err: e },
                //     'Encountered innoculous Kafka error: ' +
                //     `'${e.message} (${e.code}). Delaying 100 ms before continuing.`
                // );
                // Delay a small amount after innoculous errors to keep
                // the consume loop from being so busy when there are no
                // new messages to consume.
                return P.delay(100);
            default:
                // console.log(
                //     { err: e },
                //     'Caught Kafka error while attempting to consume a message.'
                // );
                throw e;
        }
    })
    // Any unexpected error will be thrown to the client will not be caught
    // here.

    // If we found a message, return it, else keep looking.
    .then((message) => {
        if (message) {
            // console.log({ message: message }, 'Consumed message.');
            return message;
        }
        else {
            // console.log('Have not yet found a message while consuming, trying again.');
            return consume(kafkaConsumer, deserializer, matcher);
        }
    });
}


function consumeLoop(kafkaConsumer, deserializer, matcher, cb) {
    // Consume, call cb with message, then consume again
    return consume(kafkaConsumer, deserializer, matcher)
    .then((message) => {
        cb(undefined, message);
        return consumeLoop(kafkaConsumer, deserializer, matcher, cb);
    })
    .catch((e) => {
        return cb(e, undefined);
    });
    // better to stop on error?  not sure.
    // .finally(() => {
    //     consumeLoop(kafkaConsumer, res);
    // });
}



module.exports = {
    createKafkaConsumerAsync:       createKafkaConsumerAsync,
    getAvailableTopics:             getAvailableTopics,
    validateAssignments:            validateAssignments,
    buildAssignments:               buildAssignments,
    deserializeKafkaMessage:        deserializeKafkaMessage,
    consume:                        consume,
    consumeLoop:                    consumeLoop,
};
