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
    for (let j = 0; j < 10; j++) {
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

//move recent device data to mongodb and pop them out of redis lists
const visitProcessor = setInterval(() => {
    var startFlushingData = new Date();
    console.log("flush...");
    client.keys("*", function (err, replies) {
        let dbDump = [];
        let deviceInstances = replies;
        //multi = client.multi();
        var currentTime = new Date();
        deviceInstances.forEach(function (deviceKey, i) {
            //begin transactions here
            client.ZREVRANGE(deviceKey, 0, -1, function (err, elements) {
                if (elements[0]) {
                    client.zscore(deviceKey, elements[0].toString(), (err, score) => {
                        if (score <= (currentTime.getTime() - 15000)) {
                            let deviceData = { deviceKey: deviceKey, Values: elements };
                            client.del(deviceKey, function () {
                                dbDump.push(deviceData);
                                //if this is the last device key index (save the dbDump to database)
                                if (i === (deviceInstances.length - 1)) {
                                    saveToDb(dbDump);
                                    var endFlushingData = new Date() - startFlushingData;
                                    console.info('10 keys copied and send to db in: %dms', endFlushingData);
                                }
                            })
                        }
                    });
                }
            })
        });
    });
}, 17000);

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
                for (let i = 0; i < element.Values.length; i++) {
                    device.raw_points.push({ timestamp: `${element.Values[i]}` });
                }
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
                for (let i = 0; i < element.Values.length; i++) {
                    deviceRes.raw_points.push({ timestamp: `${element.Values[i]}` });
                }
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
