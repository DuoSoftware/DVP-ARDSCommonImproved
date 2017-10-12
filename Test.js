/**
 * Created by Heshan.i on 10/12/2017.
 */

var resourceHandler = require('./ResourceHandler');

//resourceHandler.SetResourceLogin('test', 1, 103, 108, 'Rusiru', [{
//    "Type": "CALL",
//    "Contact": {
//        "ContactName": "9501",
//        "Domain": "duo.media1.veery.cloud",
//        "Extention": "9501",
//        "ContactType": "PRIVATE"
//    }
//}]);

resourceHandler.RemoveResource('test', 1, 103, 108);