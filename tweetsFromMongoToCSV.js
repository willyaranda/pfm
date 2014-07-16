var MongoClient = require('mongodb').MongoClient;
var csv = require("fast-csv");
var fs = require('fs');
var tweetWS = fs.openSync("tweet.csv", 'w+');
var userWS = fs.openSync("user.csv", 'w+');

MongoClient.connect('mongodb://127.0.0.1:27017/pfm', function(err, db) {
  if (err) throw err;

  var collection = db.collection('tweets');
  // Locate all the entries using find
  var cursor = collection.find();
  var count = 0;
  var handled = true;

  var stream = cursor.stream();

  stream.on('data', function(doc) {
    if (!doc) {
      console.log("Finished");
      tweetWS.close();
      userWS.close();
    }
    if (++count % 1000 === 0) console.log("Done " + count);
    fs.writeSync(tweetWS, doc.id + '\t' + doc.text.replace(/(\r\n|\n|\r)/gm,"") + '\t ' + doc.user.id + '\t ' + (doc.isRetweet ? 'true' : 'false') + '\t' + doc.user.screen_name + '\n');
    fs.writeSync(userWS, doc.user.id + '\t' + doc.user.screen_name + '\n');
  });
});