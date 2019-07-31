const express = require('express');
const redis = require('redis');
const mongoose = require('mongoose');
const Device = require("./models/Device");
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Init app
const port = 3000;
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

//Initialize randomizer
function getRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}

//Add raw data to random devices
let dataCollectorExetimes = 0;
const dataCollector = setInterval(() => {
    for (let j = 0; j < 1000; j++) {
        //simulate random devices
        var currentdate = new Date();
        let deviceKey = "device_" + j;
        let raw_point_data = "x,y_" + currentdate.getTime().toString();
        let time_stamp = currentdate.getTime().toString();
        client.zadd(deviceKey, time_stamp, raw_point_data, function () {
            //
        });
    }
    console.info('10 DataPoints collected');
    dataCollectorExetimes++;
    if (dataCollectorExetimes >= 9) {
        clearInterval(dataCollector);
    }
}, 150);

let visitProcessorCounter = 0;
let visitProcessor2Counter = 0;


//move recent device data to mongodb and pop them out of redis lists
const visitProcessor = setInterval(() => {
    client.keys("*", function (err, replies) {
        numberOfKeys = replies;
        replies.forEach((element, iterator) => {
            redisClient = redis.createClient();
            redisClient.watch(element, function (err) {
                //if (err) throw err;
                redisClient.get(element, function (err, result) {
                    redisClient.multi()
                        .del(element)
                        .exec(function (err, results) {
                            //if (err) throw err;
                            if (results == 1) {
                                visitProcessorCounter++;
                            }
                            // console.log("proc1 results: " + results + " error: " + err);
                            // if (iterator == (numberOfKeys.length - 1)) {
                            //     console.log("visitProcessorCounter: " + visitProcessorCounter);
                            // }
                        });
                });
            });

        });

    });
}, 17000);

const visitProcessor2 = setInterval(() => {
    client.keys("*", function (err, replies) {
        numberOfKeys = replies;
        replies.forEach((element, iterator) => {
            redisClient1 = redis.createClient();
            redisClient1.watch(element, function (err) {
                //if (err) throw err;
                redisClient1.get(element, function (err, result) {
                    redisClient1.multi()
                        .del(element)
                        .exec(function (err, results) {
                            //if (err) throw err;
                            if (results == 1) {
                                visitProcessor2Counter++;
                            }
                            // console.log("proc2 results: " + results + " error: " + err);
                            // if (iterator == (numberOfKeys.length - 1)) {
                            //     console.log("visitProcessor2Counter: " + visitProcessor2Counter);
                            // }
                        });
                });
            });

        });
    });
}, 17000);


const aftermath = setInterval(() => {
    console.log("visitProcessorCounter: " + visitProcessorCounter);
    console.log("visitProcessor2Counter: " + visitProcessor2Counter);
}, 19000)