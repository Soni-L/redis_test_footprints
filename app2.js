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

//Initialize randomizer
function getRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}

//Add raw data to random devices
// const dataCollector = setInterval(() => {
//     for (let j = 0; j < 10; j++) {
//         //simulate random devices
//         var currentdate = new Date();
//         let deviceKey = "device_" + j;
//         let raw_point_data = "x,y_" + currentdate.getTime().toString();
//         let time_stamp = currentdate.getTime().toString();
//         //push keys in devices
//         // client.rpush(deviceKey, raw_point_data, function () {

//         // });
//         client.zadd(deviceKey, time_stamp, raw_point_data, function () {
//             //
//         });
//     }
//     console.info('10 DataPoints collected');
// }, 1500);

//move recent device data to mongodb and pop them out of redis lists
const visitProcessor = setInterval(() => {
    var startFlushingData = new Date();
    console.log("flush...");
    //clearInterval(dataCollector);
    client.keys("*", function (err, replies) {
        let dbDump = [];
        let deviceInstances = replies;
        multi = client.multi();
        deviceInstances.forEach(function (deviceKey1, i) {
            client.ZREVRANGE(deviceKey1, 0, -1, function (err, elements) {
                if (elements) {
                    if (elements[0]) {
                        let deviceData = { deviceKey: deviceKey1, Values: elements[0] };
                        multi.del(deviceKey1, function () {
                            //
                        })
                        multi.exec(function (err, replies) {
                            dbDump.push(deviceData);
                            if (i === (deviceInstances.length - 1)) {
                                //     client.flushdb(function () {
                                saveToDb(dbDump);
                                //clearInterval(visitProcessor);
                                //client.quit();  
                                var endFlushingData = new Date() - startFlushingData;
                                console.info('10 keys copied and send to db in: %dms', endFlushingData);
                                //     });
                            }
                        });
                    }
                }
            })

        });
    });
}, 15000);

function saveToDb(objectArray) {
    //start timer
    var startFlushingDataToDb = new Date();

    const data = []
    for (let i = 0; i < objectArray.length; i++) {
        data.push({ device_ID: objectArray[i].deviceKey, device_data: objectArray[i].Values })
    }

    objectArray.forEach((element, iteretor) => {
        // query = { device_id: `${element.deviceKey}` };
        Device.findOne({ device_id: `${element.deviceKey}` }, function (err, deviceRes) {
            if (!deviceRes && (element.Values != null)) {
                var device = new Device({ device_id: `${element.deviceKey}` });
                device.raw_points.push({ timestamp: `${element.Values} app2` });
                device.save(function (err, document) {
                    console.log(document);
                });
                if (iteretor === (objectArray.length - 1)) {
                    var endFlushingDataToDb = new Date() - startFlushingDataToDb;
                    console.info('All visitst saved to db in: %dms', endFlushingDataToDb);
                }
                console.log('device ' + deviceRes + " error: " + err);
            }
            if (deviceRes && (element.Values != null)) {
                deviceRes.raw_points.push({ timestamp: `${element.Values} app2` });
                deviceRes.save(function (err, document) {
                });
                //console.log('device ' + deviceRes);
                if (iteretor === (objectArray.length - 1)) {
                    var endFlushingDataToDb = new Date() - startFlushingDataToDb;
                    console.info('All visitst updated to db in: %dms', endFlushingDataToDb);
                }
                console.log('device ' + deviceRes + " error: " + err);
            }

        });

    });
}