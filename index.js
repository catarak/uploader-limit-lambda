"use strict";
// assumes process.env.MONGODB_URI and processing.env.MONGODB_NAME are defined in your
// lambda's configuration
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const mongo = require('mongodb');
const MongoClient = mongo.MongoClient;
const MONGODB_URI = process.env.MONGODB_URI;

let cachedDb = null;

function connectToDatabase(uri) {
    console.log('=> connect to database');

    if (cachedDb) {
        console.log('=> using cached database instance');
        return Promise.resolve(cachedDb);
    }

    return MongoClient.connect(uri, { useNewUrlParser: true })
        .then(db => {
            console.log('connected to database!')
            cachedDb = db.db(process.env.MONGODB_NAME);
            return cachedDb;
        });
}

function queryDatabase(db, userId, bytes) {
    console.log('=> query database');
    console.log(userId);
    return db.collection('users').findOneAndUpdate(
        { _id: mongo.ObjectID(userId) },
        { $set: { totalSize: bytes } }
        )
        .then((users) => { 
            return { statusCode: 200, body: users };
        })
        .catch(err => {
            console.log('=> an error occurred: ', err);
            return { statusCode: 500, body: 'error' };
        });
}

exports.handler = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    var srcBkt = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    var userId = srcKey.split('/')[0];
    
    s3.listObjects({
        Bucket: srcBkt,
        Prefix: `${userId}/`
    }, function (err, data) {
        if (err) {
            console.log(err);
        }
        const totalSize = data.Contents.reduce((acc, obj) => acc + obj.Size, 0);
        console.log(totalSize);
        
        connectToDatabase(MONGODB_URI)
            .then(db => queryDatabase(db, userId, totalSize))
            .then(result => {
                console.log('=> returning result: ', result);
                callback(null, result);
                context.succeed("done");
            })
            .catch(err => {
                console.log('=> an error occurred: ', err);
                callback(err);
            });
    });
};

