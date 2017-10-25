/**
 * Created by Heshan.i on 10/12/2017.
 */

var resourceHandler = require('./ResourceHandler');
var requestServerHandler = require('./RequestServerHandler');
var requestMetadataHandler = require('./RequestMetaDataHandler');

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

//resourceHandler.GetResourcesByTags('test', 1, 103, '', '', '').then(function (result) {
//    console.log(JSON.stringify(result));
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Update Slot State Reserved-------------------------

//resourceHandler.UpdateSlotStateReserved('test', 1, 103, 129, 'CALL', 0, '123123', 30, 10, 30, 3, '').then(function (result) {
//    console.log(JSON.stringify(result));
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Update Slot State Reserved-------------------------

//resourceHandler.UpdateSlotStateBySessionId('test', 1, 103, 129, 'CALL', '123123', 'completed', '', '', 'outbound').then(function (result) {
//    console.log(JSON.stringify(result));
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Add Request Server-------------------------

//requestServerHandler.AddRequestServer('test', 1, 103, {
//    "ServerType":"CALLSERVER",
//    "RequestType":"CALL",
//    "CallbackUrl":"http://localhost:2228/api/Result",
//    "CallbackOption":"POST",
//    "QueuePositionCallbackUrl":"http://localhost:2228/api/QueuePosition",
//    "ReceiveQueuePosition":true,
//    "ServerID":"10"
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});

//--------------Add Request Metadata-------------------------

//requestMetadataHandler.AddRequestMetaData('test', 1, 103, {
//    "ServerType":"CALLSERVER",
//    "RequestType":"CALL",
//    "ServingAlgo":"CALLBACK",
//    "AttributeGroups":[55,57],
//    "HandlingAlgo":"SINGLE",
//    "SelectionAlgo":"BASIC",
//    "MaxReservedTime":10,
//    "MaxRejectCount":1500,
//    "MaxAfterWorkTime":100,
//    "ReqHandlingAlgo":"QUEUE",
//    "ReqSelectionAlgo":"LONGESTWAITING"
//}).then(function (result) {
//    console.log(result);
//}).catch(function (ex) {
//    console.log(ex);
//});


//----------------------Add Request----------------------------

//preProcessRequestData('test', 1, 103, {
//        "ServerType": "CALLSERVER",
//        "RequestType": "CALL",
//        "SessionId": "1111",
//        "Attributes": ["64", "60", "61"],
//        "RequestServerId": "10",
//        "Priority": "0",
//        "ResourceCount": 1,
//        "OtherInfo": ""
//    }
//).then(function (result) {
//        console.log(JSON.stringify(result));
//    }).catch(function (ex) {
//        console.log(ex);
//    });