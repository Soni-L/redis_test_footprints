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
    client.keys("*", function (err, replies) {
        let dbDump = [];
        let deviceInstances = replies;
        //multi = client.multi();
        var currentTime = new Date();
        replies.forEach((deviceKey, i) => {
            //begin transactions here
            client.ZREVRANGE(deviceKey, 0, -1, function (err, elements) {
                if (elements[0]) {
                    client.zscore(deviceKey, elements[0].toString(), (err, score) => {
                        if (score <= (currentTime.getTime() - 15000)) {
                            client.watch(deviceKey, function (err) {
                                let deviceData = { deviceKey: deviceKey, Values: elements };
                                client.multi()
                                    .del(deviceKey)
                                    .exec(function (err, results) {
                                        if (results == 1) {
                                            dbDump.push(deviceData);
                                        }
                                        if (i === (replies.length - 1)) {
                                            var endFlushingData = new Date() - startFlushingData;
                                            console.info(`app2 ${dbDump.length} keys copied and send for db save in: ${endFlushingData} ms`);
                                            console.log(`app2 memory usage before Save DB, RSS ${humanFileSize(process.memoryUsage().rss)}, heapTotal ${humanFileSize(process.memoryUsage().heapTotal)}, heapUsed ${humanFileSize(process.memoryUsage().heapUsed)}, external ${humanFileSize(process.memoryUsage().external)}`);
                                            saveToDb(dbDump);
                                        }
                                    })
                            })
                        }
                    });
                }
            })
        });
    });
}, 60000);

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
    console.log(`App2 memory usage after Save DB, RSS ${humanFileSize(process.memoryUsage().rss)}, heapTotal ${humanFileSize(process.memoryUsage().heapTotal)}, heapUsed ${humanFileSize(process.memoryUsage().heapUsed)}, external ${humanFileSize(process.memoryUsage().external)}`);
}

function humanFileSize(size) {
    var i = Math.floor(Math.log(size) / Math.log(1024));
    return (size / Math.pow(1024, i)).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
};