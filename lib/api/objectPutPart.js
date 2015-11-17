import utils from '../utils.js';
import services from '../services.js';
import async from 'async';


/**
 * PUT part of object during a multipart upload. Steps include:
 * validating metadata for authorization, bucket existence
 * and multipart upload initiation existence,
 * store object data in datastore upon successful authorization,
 * store object location returned by datastore in metadata and
 * return the result in final callback
 *
 * @param  {string}   accessKey - user access key
 * @param  {datastore}   datastore - data storage endpoint
 * @param  {metastore}   metastore - metadata storage endpoint
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} callback  - final callback to call with the result
 * @return {function} calls callback from router
 * with err and result as arguments
 */
export default
function objectPutPart(accessKey, datastore, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const contentMD5 = request.calculatedMD5;
    const size = request.lowerCaseHeaders['content-length'];
    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return callback('TooManyParts');
    }
    if (!Number.isInteger(partNumber)) {
        return callback('InvalidArgument');
    }
    // If part size is greater than 5GB, reject it
    if (Number.parseInt(size, 10) > 5368709120) {
        return  callback('EntityTooLarge');
    }
    // Note: Parts are supposed to be at least 5MB except for last part.
    // However, there is no way to know whether a part is the last part
    // since keep taking parts until get a completion request.  But can
    // expect parts of at least 5MB until last part.  Also, we check that
    // part sizes are large enough when mutlipart upload completed.

    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;
    const partUID =
        utils.getResourceUID(
            request.namespace, uploadId + partNumber);
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        uploadId,
    };
    const dataStoreParams = {
        contentMD5,
        partUID,
        headers: request.lowerCaseHeaders,
        value: request.post,
    };
    const metaStoreParams = {
        partNumber,
        contentMD5,
        size,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateMultipart(metadataValParams, next);
        },
        function waterfall2(bucket, multiPartMetadata, next) {
            services.dataStore(bucket, multiPartMetadata,
            datastore, dataStoreParams, next);
        },
        function waterfall3(bucket, multiPartMetadata, newLocation, next) {
            services.metadataStorePart(bucket, multiPartMetadata, newLocation,
            metaStoreParams, next);
        }
    ], function watefallFinal(err) {
        return callback(err, contentMD5);
    });
}