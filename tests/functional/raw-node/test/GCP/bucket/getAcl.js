const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const credentialTwo = 'gcpbackend2';

const projectGroupId = 'project-owners-750004687128';
const projectGroupName = 'Project 750004687128 owners';

const aclExpectedObject = {
    Owner: {
        ID: 'project-owners-750004687128',
        DisplayName: 'Project 750004687128 owners' },
    Grants: [
        {
            Grantee: {
                Type: 'GroupById',
                ID: projectGroupId,
                DisplayName: projectGroupName,
            },
            Permission: 'FULL_CONTROL',
        },
        {
            Grantee: {
                Type: 'AllUsers',
            },
            Permission: 'WRITE',
        },
    ],
};

describe('GCP: GET Bucket ACL', function testSuite() {
    this.timeout(8000);
    let config;
    let gcpClient;

    before(() => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.bucketName = `somebucket-${Date.now()}`;
        makeGcpRequest({
            method: 'PUT',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
            headers: {
                'x-goog-acl': 'public-read-write',
            },
        }, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
            } else {
                process.stdout.write('Created bucket\n');
            }
            return done(err);
        });
    });

    afterEach(function afterFn(done) {
        makeGcpRequest({
            method: 'DELETE',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            } else {
                process.stdout.write('Deleted bucket\n');
            }
            return done(err);
        });
    });

    describe('when user does not have ACL permissions', () => {
        let gcpClient2;

        before(() => {
            const config2 = getRealAwsConfig(credentialTwo);
            gcpClient2 = new GCP(config2);
        });

        it('should return 403 and AccessDenied', function testFn(done) {
            gcpClient2.getBucketAcl({
                Bucket: this.test.bucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 403);
                assert.strictEqual(err.code, 'AccessDenied');
                return done();
            });
        });
    });

    describe('when user has ACL permissions', () => {
        it('should retrieve correct ACP', function testFn(done) {
            return gcpClient.getBucketAcl({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                assert.deepStrictEqual(res, aclExpectedObject);
                return done();
            });
        });
    });
});
