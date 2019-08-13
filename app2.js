const express = require('express');
const redis = require('redis');
const mongoose = require('mongoose');
const Device = require("./models/Device");
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Init app
const port = 3001;
const app = express();

//Listen on port
app.listen(port, function () {
    console.log('Server started on port ' + port);
    console.log("Node PID: " + process.pid);
});

// Create Redis Client
let client = redis.createClient();
client.on('connect', function () {
    console.log('Connected to Redis...');
});

//connect to the database
const server = '127.0.0.1:27017'; // REPLACE WITH YOUR DB SERVER
const database = 'redis_tests';      // REPLACE WITH YOUR DB NAME

mongoose.connect(`mongodb://${server}/${database}`);
var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function () {
    console.info('Mongo connection succeded');
});


//move recent device data to mongodb and pop them out of redis lists
const visitProcessor = setInterval(() => {
    var startFlushingData = new Date();
    var currentTime = new Date();
    let dbDump = [];
    var cursor = '0';
    function scan() {
        //Match all keys and return 10 items at a time
        client.scan(cursor, 'MATCH', '*', 'COUNT', '10', function (err, reply) {
            if (err) {
                throw err;
            }
            cursor = reply[0];
            if (cursor === '0') {
                //End of the line  -- all keys were scanned
                var endFlushingData = new Date() - startFlushingData;
                console.info(`${dbDump.length} keys copied and send for db save in: ${endFlushingData} ms`);
                saveToDb(dbDump);
                return console.log('Scan Complete');
            } else {
                // reply[1] is an array of matched keys for the current query slice
                for (let i = 0; i < reply[1].length; i++) {

                    let lastAccessCurrentKey = 0;
                    //begin transaction
                    client.multi();
                    //Check the last time a key was accessed (timestamp)
                    client.object("IDLETIME", reply[1][i], function (err, lastAccess) {
                        //touch the current key to reset its last-access time
                        //effectively blocking the other node instance processing it
                        client.LLEN(reply[1][i], function (err) {
                            client.exec(function (err, results) {
                                if (results === null) {
                                    console.log('transaction null, skipping element');
                                } else {
                                    lastAccessCurrentKey = lastAccess;
                                    console.log('transaction worked, processing element...' + lastAccessCurrentKey);
                                }
                            });
                        });
                    });

                    //begin processing element (if condition allows)
                    if (lastAccessCurrentKey <= (currentTime.getTime() - 15000)) {
                        //second transaction
                        client.watch(reply[1][i]);
                        client.LRANGE(reply[1][i], 0, -1, function (err, elements) {
                            let deviceData = { deviceKey: reply[1][i], Values: elements };
                            client.multi()
                                .del(reply[1][i])
                                .exec(function (err, results) {
                                    if (results === null) {
                                        console.log('transaction aborted because results were null');
                                    } else {
                                        dbDump.push(deviceData);
                                        console.log('transaction worked and returned', results);
                                    }
                                });
                        });
                    }
                }
                return scan();
            }
        });
    }
    scan(); //call scan function
}, 19000);

function saveToDb(objectArray) {
    //start timer
    var startFlushingDataToDb = new Date();
    let bulk = Device.collection.initializeUnorderedBulkOp();
    objectArray.forEach((element, iteretor) => {
        bulk.find({ device_id: `${element.deviceKey}` }).upsert().update(
            {
                $set: { device_id: element.deviceKey },
                $push: { raw_points: { $each: element.Values } }
            }
        );
    });
    bulk.execute()
        .catch(err => {
            console.log("App 2 could not perform bulk upsert");
        });;
    var endFlushingDataToDb = new Date() - startFlushingDataToDb;
    console.info('App2 All visitst saved to db in: %dms', endFlushingDataToDb);
}

function humanFileSize(size) {
    var i = Math.floor(Math.log(size) / Math.log(1024));
    return (size / Math.pow(1024, i)).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
};