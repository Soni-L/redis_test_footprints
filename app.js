const express = require('express');
const path = require('path');
const redis = require('redis');
const mongoose = require('mongoose');
const csv = require('csv-parser');
const fs = require('fs');
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

//mongoose.connect("mongodb+srv://soni:cnet33kc@cluster0-enuba.mongodb.net/test?retryWrites=true&w=majority");
mongoose.connect(`mongodb://${server}/${database}`);
var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function () {
    console.info('Mongo connection succeded');
});

//define schema module
var Schema = mongoose.Schema;

//create device and embedded[rawpoint] schemas
var rawPointSchema = new Schema({ timestamp: String });
var deviceSchema = new Schema({
    device_id: String,
    raw_points: [rawPointSchema]
});

//creating the device model
let Device = mongoose.model('Device', deviceSchema);

//Initialize randomizer
function getRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}

//Add raw data to random devices
const collectDataInterval = setInterval(() => {
    for (let j = 0; j < 10; j++) {
        //simulate random devices
        var currentdate = new Date();
        let deviceKey = "device_" + j;
        let raw_point_data = "x,y_" + currentdate.getTime().toString();
        let time_stamp = currentdate.getTime().toString();
        //push keys in devices
        // client.rpush(deviceKey, raw_point_data, function () {

        // });
        client.zadd(deviceKey, time_stamp, raw_point_data, function () {
            //
        });
    }
    console.info('10 DataPoints collected');
}, 1500);

//move recent device data to mongodb and pop them out of redis lists
const flushInterval = setInterval(() => {
    var startFlushingData = new Date();
    console.log("flush...");
    //clearInterval(collectDataInterval);
    client.keys("*", function (err, replies) {
        let dbDump = [];
        replies.forEach(function (deviceKey1, i) {
            client.ZREVRANGE(deviceKey1, 0, -1, function (err, elements) {
                if (elements[0]) {
                    let deviceData = { deviceKey: deviceKey1, Values: elements[0] };
                    dbDump.push(deviceData);
                }
                if (i === (replies.length - 1)) {
                    //     client.flushdb(function () {
                    //saveToCSV(dbDump);
                    saveToDb(dbDump);
                    //clearInterval(flushInterval);
                    //client.quit();  
                    var endFlushingData = new Date() - startFlushingData;
                    console.info('10 keys copied and send to db in: %dms', endFlushingData);
                    //     });
                }
            })
        });
    });
}, 15000);

function saveToCSV(objectArray) {
    const csvWriter = createCsvWriter({
        path: 'out.csv',
        header: [
            { id: 'device_ID', title: 'device_ID' },
            { id: 'device_data', title: 'device_data' }
        ]
    });

    const data = []
    for (let i = 0; i < objectArray.length; i++) {
        data.push({ device_ID: objectArray[i].deviceKey, device_data: objectArray[i].Values })
    }
    csvWriter
        .writeRecords(data)
        .then(() => console.log('The CSV file was written successfully'));
}

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
            if (!deviceRes && (element.Values!= null)) {
                var device = new Device({ device_id: `${element.deviceKey}` })
                device.raw_points.push({ timestamp: `${element.Values}` });
                device.save(function (err, document) {
                    console.log(document);
                });
                if (iteretor === (objectArray.length - 1)) {
                    var endFlushingDataToDb = new Date() - startFlushingDataToDb;
                    console.info('All visitst saved to db in: %dms', endFlushingDataToDb);
                }
                console.log('device ' + deviceRes + " error: " + err);
            }
            if (deviceRes && (element.Values!= null)) {
                deviceRes.raw_points.push({ timestamp: `${element.Values}` });
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
