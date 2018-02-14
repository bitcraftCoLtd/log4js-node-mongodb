var log4js = require('log4js');
var lxHelpers = require('lx-helpers');
var mongodb = require('mongodb');
// This is used to identify the log
var MODULE_NAME = 'node-mongo-appender';
var logger = log4js.getLogger(MODULE_NAME);

/**
 * Returns a function to log data in mongodb.
 *
 * @param {Object} config The configuration object.
 * @param {string} config.connectionString The connection string to the mongo db.
 * @param {string=} config.layout The log4js layout.
 * @param {string=} config.write The write mode.
 * @returns {Function}
 */
function mongodbAppender(config) {
    var collection;
    var cache = [];
    var batchLogEvent = [];
    var layout = {};
    var collectionName = '';
    var connectionOptions = {};
    var metaData = {};
    // Check the inerval
    var BATCH_INTERVAL = 0;
    if (!config || !config.connectionString) {
        throw new Error('connectionString is missing. Cannot connect to mongdb.');
    }
    layout = config.layout || log4js.layouts.messagePassThroughLayout;
    collectionName = config.collectionName || 'log';
    connectionOptions = config.connectionOptions || {};
    // Inserted for extra logging purposes
    metaData = config.metaData || {};
    if (config.writeInterval && config.writeInterval > 0) {
        BATCH_INTERVAL = config.writeInterval * 1000; // Convert to ms
    }

    /**
     * @param {Object} err
    */
    function ERROR(err) {
        Error.call(this);
        Error.captureStackTrace(this, this.constructor);

        this.name = err.toString();
        this.message = err.message || 'error';
    }

    /**
     * Replace keys
    */
    function replaceKeys(src) {
        var result = {};
        /**
         * @param {Object} dest
         * @param {Object} source;
         * @param {function} cloneFunc
        */
        function mixin(dest, source, cloneFunc) {
            if (lxHelpers.isObject(source)) {
                lxHelpers.forEach(source, function (value, key) {
                    // replace $ at start
                    if (key[0] === '$') {
                        key = key.replace('$', '_dollar_');
                    }

                    // replace all dots
                    key = key.replace(/\./g, '_dot_');

                    dest[key] = cloneFunc ? cloneFunc(value) : value;
                });
            }

            return dest;
        }

        if (!src || typeof src !== 'object' || typeof src === 'function' || src instanceof Date || src instanceof RegExp || src instanceof mongodb.ObjectID) {
            return src;
        }

        // wrap Errors in a new object because otherwise they are saved as an empty object {}
        if (lxHelpers.getType(src) === 'error') {
            return new ERROR(src);
        }

        // Array
        if (lxHelpers.isArray(src)) {
            result = [];

            lxHelpers.arrayForEach(src, function (item) {
                result.push(replaceKeys(item));
            });
        }

        return mixin(result, src, replaceKeys);
    }
    /**
     * Get write option
    */
    function getOptions() {
        var options = {w: 0};

        if (config.write === 'normal') {
            options.w = 1;
        }

        if (config.write === 'safe') {
            options.w = 1;
            options.journal = true;
        }

        return options;
    }

    /**
     * @param {Object} loggingEvent
    */
    function insert(loggingEvent) {
        var options = getOptions();
        var insertedLogData = {};
        if (loggingEvent.logger.category === MODULE_NAME) {
            // Skip logging to DB if the log come from this module
            return;
        }

        insertedLogData = {
            timestamp: loggingEvent.startTime,
            data: loggingEvent.data,
            level: loggingEvent.level,
            category: loggingEvent.logger.category,
            metaData: metaData
        };

        if (BATCH_INTERVAL > 0) {
            batchLogEvent.push(insertedLogData);
            return;
        }

        if (collection) {
            if (options.w === 0) {
                // fast write
                collection.insert(insertedLogData, options);
            } else {
                // save write
                collection.insert(insertedLogData, options, function (error) {
                    if (error) {
                        // log4js.error('log: Error writing data to log!');
                        logger.error(error);
                        // log4js.error('log: Connection: %s, collection: %, data: %j', config.connectionString, collectionName, loggingEvent);
                    }
                });
            }
        } else {
            cache.push(loggingEvent);
        }
    }

    /**
     * Insert the data stored in batch insert
    */
    function batchInsert() {
        var options = getOptions();
        var insertedBatchLog = [];
        // console.debug('Do Batch insert Total log ' + batchLogEvent.length);
        // Only insert log if there are event to be logged
        if (batchLogEvent.length > 0) {
            // Clear the batch log and copy all of it's data to new array.
            // This is done to prevent new data being deleted when the insert function is running
            insertedBatchLog = batchLogEvent;
            // Set the batchLogEvent to an empty array
            batchLogEvent = [];
            if (collection) {
                // Always write with safe option
                collection.insert(insertedBatchLog, options, function (error) {
                    if (error) {
                        // log4js.error('log: Error writing data to log!');
                        logger.error(error);
                        // log4js.error('log: Connection: %s, collection: %, data: %j', config.connectionString, collectionName, loggingEvent);
                    }
                });
            } else {
                cache.push(insertedBatchLog);
            }
        }
    }

    // check connection string
    if (config.connectionString.indexOf('mongodb://') !== 0) {
        config.connectionString = 'mongodb://' + config.connectionString;
    }

    if (BATCH_INTERVAL > 0) {
        setInterval(batchInsert, BATCH_INTERVAL);
    }

    // connect to mongodb
    mongodb.MongoClient.connect(config.connectionString, connectionOptions, function (err, db) {
        if (err) {
            logger.error('Error connecting to mongodb! URL: %s', config.connectionString);
            logger.error(err);
            // Unable to connecto to db.
            return;
        }

        collection = db.collection(collectionName);

        // process cache
        cache.forEach(function (loggingEvent) {
            setImmediate(function () {
                insert(loggingEvent);
            });
        });
    });

    return function (loggingEvent) {
        var clonedEvent = JSON.parse(JSON.stringify(loggingEvent));

        // get the information to log
        if (Object.prototype.toString.call(clonedEvent.data[0]) === '[object String]') {
            // format string with layout
            clonedEvent.data = layout(clonedEvent);
        } else if (clonedEvent.data.length === 1) {
            clonedEvent.data = clonedEvent.data[0];
        }

        clonedEvent.data = replaceKeys(clonedEvent.data);

        // save in db
        insert(clonedEvent);
    };
}

/**
 * Set configuration
*/
function configure(config) {
    if (config.layout) {
        config.layout = log4js.layouts.layout(config.layout.type, config.layout);
    }

    return mongodbAppender(config);
}

exports.appender = mongodbAppender;
exports.configure = configure;
