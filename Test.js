/**
 * Created by Heshan.i on 10/12/2017.
 */

var resourceHandler = require('./ResourceHandler');

//--------------Add Resource----------------------------

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

//--------------Share Resource----------------------------

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

//--------------Remove Share Resource----------------------

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

//--------------Remove Resource----------------------------

//resourceHandler.RemoveResource('test', 1, 103, 129).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});


//--------------Set Resource Attribute Info----------------

/*{"ResourceId":129,"ResourceAttributeInfo":{"Attribute":"89","HandlingType":"CALL","Percentage":70},"OtherInfo":""}*/

//resourceHandler.SetResourceAttributes('test', 1, 103, 129, {
//    "Attribute": "89",
//    "HandlingType": "CALL",
//    "Percentage": 70
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Get Resource Info-------------------------

//resourceHandler.GetResource('test', 1, 103, 129, null).then(function (result) {
//    console.log(JSON.stringify(result));
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Get Resource Status Info-------------------------

//resourceHandler.GetResourceStatus('test', 1, 103, 129).then(function (result) {
//    console.log(JSON.stringify(result));
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Get Resource Status Info-------------------------

resourceHandler.GetResourcesByTags('test', 1, 103, '', '', '').then(function (result) {
    console.log(JSON.stringify(result));
}).catch(function (ex) {
    console.log(ex);
});