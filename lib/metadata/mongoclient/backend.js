const util = require('util');
const arsenal = require('arsenal');

const logger = require('../../utilities/logger');

const constants = require('../../../constants');
const { config } = require('../../Config');

const errors = arsenal.errors;
const versioning = arsenal.versioning;
const BucketInfo = arsenal.models.BucketInfo;

const MongoClient = require('mongodb').MongoClient;

const genVID = versioning.VersionID.generateVersionId;

const MongoReadStream = require('./readStream');

const METASTORE = '__metastore';

let uidCounter = 0;

function generateVersionId() {
    // generate a unique number for each member of the nodejs cluster
    return genVID(`${process.pid}.${uidCounter++}`,
		  config.replicationGroupId);
}

function formatVersionKey(key, versionId) {
    return `${key}\0${versionId}`;
}

function inc(str) {
    return str ? (str.slice(0, str.length - 1) +
            String.fromCharCode(str.charCodeAt(str.length - 1) + 1)) : str;
}

class MongoClientInterface {
    constructor() {
	const mongoUrl =
	      `mongodb://${config.mongodb.host}:${config.mongodb.port}`;

	this.client = null;
	this.db = null;
	console.log('connecting to', mongoUrl);
        MongoClient.connect(mongoUrl, (err, client) => {
	    if (err) {
		throw (errors.InternalError);
	    }
	    console.log('***CONNECTED TO MONGODB***');
	    this.client = client;
	    this.db = client.db(config.mongodb.database);
	    this.usersBucketHack();
	});
    }

    usersBucketHack() {
	/* Since the bucket creation API is expecting the usersBucket
           to have attributes, we pre-create the usersBucket
           attributes here (see bucketCreation.js line 36)*/
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
	this.createBucket(
            constants.usersBucket,
            usersBucketAttr, {}, err => {
                if (err) {
                    console.log('error writing usersBucket ' +
                                'attributes to metastore',
                                { error: err });
                    throw (errors.InternalError);
                }
            });
    }
    
    getCollection(name) {
	/* mongo has a problem with .. in collection names */
	if (name === constants.usersBucket)
	    name = "users__bucket";
	return this.db.collection(name);
    }
    
    createBucket(bucketName, bucketMD, log, cb) {
	console.log('mb +', bucketName);
	var c = this.getCollection(METASTORE);
	c.update({
	    _id: bucketName
	}, {
	    _id: bucketName,
	    value: bucketMD
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }

    getBucketAttributes(bucketName, log, cb) {
	console.log('gba +', bucketName);
	var c = this.getCollection(METASTORE);
	c.findOne({
	    _id: bucketName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchBucket);
	    }
	    return cb(null, doc.value);
	});
    }

    getBucketAndObject(bucketName, objName, params, log, cb) {
	console.log('gboa +', bucketName, objName);
	this.getBucketAttributes(bucketName, log, (err, bucket) => {
	    if (err) {
		return cb(err);
	    }
	    if (params && params.versionId) {
                objName = formatVersionKey(objName, params.versionId);
            }
	    this.getObject(bucketName, objName, params, log, (err, obj) => {
		if (err) {
		    if (err === errors.NoSuchKey) {
			return cb(null,
				  { bucket:
				    BucketInfo.fromObj(bucket).serialize()
				  });
		    } else {
			return cb(err);
		    }
		}
		return cb(null, {
                    bucket: BucketInfo.fromObj(bucket).serialize(),
                    obj: JSON.stringify(obj)
		});
	    });
	});
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
	console.log('pba +', bucketName);
	var c = this.getCollection(METASTORE);
	c.update({
	    _id: bucketName
	}, {
	    _id: bucketName,
	    value: bucketMD
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }

    deleteBucket(bucketName, log, cb) {
	console.log('db +', bucketName);
    }

    /**
     * In this case we generate a versionId and atomically 
     * and simultaneously create the object AND update the head
     */
    putObjectVerCase1(c, bucketName, objName, objVal, params, log, cb) {
	const versionId = generateVersionId();
        objVal.versionId = versionId;
	const vObjName = formatVersionKey(objName, versionId);
	c.bulkWrite([{
	    updateOne: {
		filter: {
		    _id: vObjName,
		},
		update: {
		    _id: vObjName, value: objVal
		},
		upsert: true
	    }
	}, {
	    updateOne: {
		filter: {
		    _id: objName,
		},
		update: {
		    _id: objName, value: objVal
		},
		upsert: true
	    }
	}], {
	    ordered: 1
	}, () => {
	    return cb(null, `{"versionId": "${versionId}"}`);
	});
    }

    /**
     * Case used when versioning has been disabled after objects
     * have been created with versions
     */
    putObjectVerCase2(c, bucketName, objName, objVal, params, log, cb) {
	const versionId = generateVersionId();
        objVal.versionId = versionId;
	c.update({
	    _id: objName
	}, {
	    _id: objName,
	    value: objVal
	}, {
	    upsert: true
	}, () => {
	    return cb(null, `{"versionId": "${objVal.versionId}"}`);
	});
    }

    /**
     * In this case the aller provides a versionId. This function will
     * atomically and simultaneously update the object with given
     * versionId AND the head iff the provided versionId matches the
     * one of the head
     */
    putObjectVerCase3(c, bucketName, objName, objVal, params, log, cb) {
	objVal.versionId = params.versionId;
	vObjName = formatVersionKey(objName, params.versionId);
	c.bulkWrite([{
	    updateOne: {
		filter: {
		    _id: vObjName,
		},
		update: {
		    _id: vObjName, value: objVal
		},
		upsert: true
	    }
	}, {
	    updateOne: {
		filter: {
		    _id: objName,
		    "value.versionId": params.versionId
		},
		update: {
		    _id: objName, value: objVal
		},
		upsert: true
	    }
	}], {
	    ordered: 1
	}, () => {
	    return cb(null, `{"versionId": "${objVal.versionId}"}`);
	});
    }

    /**
     * Put object when versioning is not enabled
     */
    putObjectNoVer(c, bucketName, objName, objVal, params, log, cb) {
	c.update({
	    _id: objName
	}, {
	    _id: objName,
	    value: objVal
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }
    
    putObject(bucketName, objName, objVal, params, log, cb) {
	console.log('po +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versioning) {
	    return this.putObjectVerCase1(c, bucketName, objName, objVal,
					  params, log, cb);
        } else if (params && params.versionId === '') {
	    return this.putObjectVerCase2(c, bucketName, objName, objVal,
					  params, log, cb);
        } else if (params && params.versionId) {
	    return this.putObjectVerCase3(c, bucketName, objName, objVal,
					  params, log, cb);
        } else {
	    return this.putObjectNoVer(c, bucketName, objName, objVal,
				       params, log, cb);
	}
    }

    getObject(bucketName, objName, params, log, cb) {
	console.log('go +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versionId) {
            objName = formatVersionKey(objName, params.versionId);
        }
	c.findOne({
	    _id: objName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchKey);
	    }
	    return cb(null, doc.value);
	});
    }

    // XXX rework this crap
    // use https://docs.mongodb.com/manual/tutorial/perform-two-phase-commits/
    deleteObjectVerMaster(c, bucketName, objName, params, log, cb) {
	const baseKey = inc(formatVersionKey(objName, ''));
	c.find({
	    _id: {
		$lt: baseKey,
		$gt: objName
	    }
	})
	    .sort({
		_id: 1
	    }, (err, keys) => {
		if (keys.length === 0) {
		    c.remove({
			_id: objName
		    }, true, (err, result) => {
			if (err)
			    return cb(errors.InternalError);
			return cb();
		    });
		}
		const key = keys.sort()[0];
		c.findOne({
		    _id: key
		}, (err, value) => {
		    c.update({
			_id: objName
		    }, {
			_id: objName,
			value: value
		    }, {
			upsert: true
		    }, () => {
			return cb()
		    });
		});
	    });
    }

    // XXX rework this crap
    deleteObjectVer(c, bucketName, objName, params, log, cb) {
	const vObjName = formatVersionKey(objName, params.versionId);
	c.findOne({
	    _id: objName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchKey);
	    }
	    c.remove({
		_id: vObjName
	    }, true, (err, result) => {
		if (err)
		    return cb(errors.InternalError);
		c.findOne({
		    _id: objName
		}, (err, mst) => {
		    console.log(err, mst);
		    if (err)
			return cb(errors.InternalError);
		    if (!mst) {
			return cb(errors.NoSuchKey);
		    }
		    if (mst.versionId === params.versionId) {
			return deleteObjectVerMaster(c, bucketName, objName,
						     params, log, cb);
		    }
		    return cb();
		});
	    });
	});
    }

    /**
     * Atomically delete an object when versioning is not enabled
     */
    deleteObjectNoVer(c, bucketName, objName, params, log, cb) {
	c.findOneAndDelete({
	    _id: objName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchKey);
	    }
	    return cb(null);
	});
    }
    
    deleteObject(bucketName, objName, params, log, cb) {
	console.log('do +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versionId) {
	    return this.deleteObjectVer(c, bucketName, objName,
					params, log, cb);
	} else {
	    return this.deleteObjectNoVer(c, bucketName, objName,
					  params, log, cb);
	}
    }
    
    internalListObject(bucketName, params, log, cb) {
        const extName = params.listingType;
        const extension = new arsenal.algorithms.list[extName](params, log);
        const requestParams = extension.genMDParams();
	var c = this.getCollection(bucketName);
        let cbDone = false;
        let stream = new MongoReadStream(c, requestParams);
        stream
            .on('data', e => {
                if (extension.filter(e) < 0) {
                    stream.emit('end');
                    stream.destroy();
                }
            })
            .on('error', err => {
                if (!cbDone) {
                    cbDone = true;
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error listing objects', logObj);
                    cb(errors.InternalError);
                }
            })
            .on('end', () => {
                if (!cbDone) {
                    cbDone = true;
                    const data = extension.result();
                    cb(null, data);
                }
            });
        return undefined;
    }

    listObject(bucketName, params, log, cb) {
	console.log('lo +', bucketName);
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
	console.log('lmpu +', bucketName);
        return this.internalListObject(bucketName, params, log, cb);
    }
}

module.exports = MongoClientInterface;
