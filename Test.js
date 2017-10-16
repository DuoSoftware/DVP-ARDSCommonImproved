/**
 * Created by Heshan.i on 10/12/2017.
 */

var resourceHandler = require('./ResourceHandler');

//resourceHandler.AddResource('test', 1, 103, 129, 'Rusiru', {
//    "Type": "CALL",
//    "Contact": {
//        "ContactName": "9501",
//        "Domain": "duo.media1.veery.cloud",
//        "Extention": "9501",
//        "ContactType": "PRIVATE"
//    }
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});

//resourceHandler.ShareResource('test', 1, 104, 129, 'Rusiru', {
//    "Type": "CALL",
//    "Contact": {
//        "ContactName": "9501",
//        "Domain": "duo.media1.veery.cloud",
//        "Extention": "9501",
//        "ContactType": "PRIVATE"
//    }
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});

//resourceHandler.RemoveShareResource('test', 1, 104, 129, {
//    "Type": "CALL",
//    "Contact": {
//        "ContactName": "9501",
//        "Domain": "duo.media1.veery.cloud",
//        "Extention": "9501",
//        "ContactType": "PRIVATE"
//    }
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});

resourceHandler.RemoveResource('test', 1, 103, 129).then(function (result) {
    console.log(result);
}).catch(function (ex) {
    console.log(ex);
});