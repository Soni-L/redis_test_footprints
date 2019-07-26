const mongoose = require('mongoose');

//define schema module
var Schema = mongoose.Schema;

//create device and embedded[rawpoint] schemas
var rawPointSchema = new Schema({ timestamp: String });
var deviceSchema = new Schema({
    device_id: String,
    raw_points: [rawPointSchema]
});

//creating the device model
const Device = mongoose.model('Device', deviceSchema);

module.exports = Device;