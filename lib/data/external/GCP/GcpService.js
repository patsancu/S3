const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');
const { grantProps, cannedAclGcp, gcpGrantTypes, awsGrantMapping,
    awsAcpMapping, permissionsAwsToGcp } = require('./GcpUtils');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    upload(params, options, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: upload not implemented'));
    },

    // Service API
    listBuckets(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listBuckets not implemented'));
    },

    // Bucket APIs

    _hasProperty(params) {
        const gotProps = [];
        Object.keys(grantProps).forEach(property => {
            if (property in params) {
                gotProps.push(property);
            }
        });
        return gotProps;
    },

    _getKeyFormat(key) {
        const awsKeyMap = awsGrantMapping[key];
        if (awsKeyMap) {
            return { type: awsKeyMap, field: gcpGrantTypes[awsKeyMap] };
        }
        return { type: key, field: gcpGrantTypes[key] };
    },

    _parseGrantHeaders(grantParams, params) {
        const accessControlPolicy = [];
        grantParams.forEach(grantName => {
            const itemList = params[grantName];
            const itemArray = itemList.split(',').map(item => item.trim());
            itemArray.forEach(item => {
                const arr = item.split('=');
                const key = this._getKeyFormat(arr[0]);
                const value = arr.length > 1 && arr[1] || '';
                const granteeObject = {
                    Grantee: {
                        Type: key.type,
                    },
                    Permission: grantProps[grantName],
                };
                if (key.field && key.field === 'emailAddress' && value) {
                    granteeObject.Grantee.EmailAddress = value;
                } else if (key.field && key.field === 'domain' && value) {
                    granteeObject.Grantee.Domain = value;
                } else if (key.field && key.field === 'id' && value) {
                    granteeObject.Grantee.ID = value;
                }
                accessControlPolicy.push(granteeObject);
            });
        });
        return accessControlPolicy;
    },

    _mapUserAcp(userAcp) {
        const mappedAcp = userAcp;
        mappedAcp.Grants = userAcp.Grants.map(grantee => {
            const retGrantee = grantee;
            retGrantee.Grantee.Type =
                awsAcpMapping[grantee.Grantee.Type] || grantee.Grantee.Type;
            retGrantee.Permission = permissionsAwsToGcp[grantee.Permission];
            return retGrantee;
        });
        return mappedAcp;
    },

    putBucketAcl(params, callback) {
        const mappedParams = {
            Bucket: params.Bucket,
            ProjectId: params.ProjectId,
        };
        const grantParams = this._hasProperty(params);
        if (grantParams.length) {
            mappedParams.AccessControlPolicy = {
                Grants: this._parseGrantHeaders(grantParams, params, callback),
            };
        } else if (params.ACL) {
            mappedParams.ACL = (cannedAclGcp[params.ACL] && params.ACL);
        } else if (params.AccessControlPolicy) {
            mappedParams.AccessControlPolicy =
                this._mapUserAcp(params.AccessControlPolicy);
        }
        return this.putBucketAclReq(mappedParams, callback);
    },

    putObjectAcl(params, callback) {
        const mappedParams = {
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: params.VersionId,
            ContentMD5: params.ContentMD5,
            ProjectId: params.ProjectId,
        };
        const grantParams = this._hasProperty(params);
        if (grantParams.length) {
            mappedParams.AccessControlPolicy = {
                Grants: this._parseGrantHeaders(grantParams, params, callback),
            };
        } else if (params.ACL) {
            mappedParams.ACL = (cannedAclGcp[params.ACL] && params.ACL);
        } else if (params.AccessControlPolicy) {
            mappedParams.AccessControlPolicy =
                this._mapUserAcp(params.AccessControlPolicy);
        }
        return this.putObjectAclReq(mappedParams, callback);
    },

    getBucketLocation(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketLocation not implemented'));
    },

    deleteBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucket not implemented'));
    },

    headBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headBucket not implemented'));
    },

    listObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjects not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjecVersions not implemented'));
    },

    putBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucket not implemented'));
    },

    putBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketWebsite not implemented'));
    },

    getBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketWebsite not implemented'));
    },

    deleteBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketWebsite not implemented'));
    },

    putBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketCors not implemented'));
    },

    getBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketCors not implemented'));
    },

    deleteBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketCors not implemented'));
    },

    // Object APIs
    headObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headObject not implemented'));
    },

    putObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObject not implemented'));
    },

    getObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getObject not implemented'));
    },

    deleteObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObject not implemented'));
    },

    deleteObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjects not implemented'));
    },

    copyObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: copyObject not implemented'));
    },

    putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implemented'));
    },

    deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implemented'));
    },

    // Multipart upload
    abortMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: abortMultipartUpload not implemented'));
    },

    createMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: createMultipartUpload not implemented'));
    },

    completeMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: completeMultipartUpload not implemented'));
    },

    uploadPart(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPart not implemented'));
    },

    uploadPartCopy(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPartCopy not implemented'));
    },

    listParts(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: listParts not implemented'));
    },
});

Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = GCP;
