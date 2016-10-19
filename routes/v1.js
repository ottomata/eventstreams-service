'use strict';

const sUtil = require('../lib/util');
const kafkaSse = require('kafka-sse');

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

/**
 * GET /topics/{topics}
 */
router.get('/topics/:topics', (req, res) => {
    kafkaSse(req, res, req.params.topics, {
        allowedTopics: app.conf.allowed_topics,
        logger: req.logger._logger,
        kafkaConfig: app.conf.kafka
    });
});

module.exports = function(appObj) {

    app = appObj;

    console.log('getting v1 route');
    return {
        path: '/v1',
        api_version: 1,
        skip_domain: true,
        router: router
    };
};

