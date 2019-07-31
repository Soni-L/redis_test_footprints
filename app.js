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
    //console.info('DataPoints collected');
    dataCollectorExetimes++;
    if (dataCollectorExetimes >= 50) {
        clearInterval(dataCollector);
    }
}, 10);

//move recent device data to mongodb and pop them out of redis lists
const visitProcessor = setInterval(() => {
    var startFlushingData = new Date();
    client.info.me
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
                                            console.info(`${dbDump.length} keys copied and send for db save in: ${endFlushingData} ms`);
                                            console.log(`app1 memory usage before Save DB, RSS ${humanFileSize(process.memoryUsage().rss)}, heapTotal ${humanFileSize(process.memoryUsage().heapTotal)}, heapUsed ${humanFileSize(process.memoryUsage().heapUsed)}, external ${humanFileSize(process.memoryUsage().external)}`);
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
}, 18000);

function saveToDb(objectArray) {
    //start timer
    var startFlushingDataToDb = new Date();
    objectArray.forEach((element, iteretor) => {
        Device.findOne({ device_id: `${element.deviceKey}` }, function (err, deviceRes) {
            if (!deviceRes && (element.Values != null)) {
                var device = new Device({ device_id: `${element.deviceKey}` });
                for (let i = 0; i < element.Values.length; i++) {
                    device.raw_points.push({ timestamp: `${element.Values[i]}` });
                }
                device.save(function (err, document) {
                    //console.log(document);
                });
            }
            if (deviceRes && (element.Values != null)) {
                for (let i = 0; i < element.Values.length; i++) {
                    deviceRes.raw_points.push({ timestamp: `${element.Values[i]}` });
                }
                deviceRes.save(function (err, document) {
                });
            }
        });
    });
    var endFlushingDataToDb = new Date() - startFlushingDataToDb;
    console.info('All visitst updated to db in: %dms', endFlushingDataToDb);
    console.log(`app1 memory usage after Update DB, RSS ${humanFileSize(process.memoryUsage().rss)}, heapTotal ${humanFileSize(process.memoryUsage().heapTotal)}, heapUsed ${humanFileSize(process.memoryUsage().heapUsed)}, external ${humanFileSize(process.memoryUsage().external)}`);
}


setInterval(() => {
    client.info((req, res) => {
        res.split("\n").map((line) => {
            if (line.match(/used_memory_human/)) {
                console.log('Used redis memory: ' + line.split(":")[1]);
                //console.log("datacoll: " + dataCollectorExetimes);
            }
        })
    });
}, 1000);

function humanFileSize(size) {
    var i = Math.floor(Math.log(size) / Math.log(1024));
    return (size / Math.pow(1024, i)).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
};