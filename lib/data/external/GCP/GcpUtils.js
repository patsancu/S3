const grantProps = {
    GrantFullControl: 'FULL_CONTROL',
    GrantRead: 'READ',
    GrantReadACP: 'FULL_CONTROL',
    GrantWrite: 'WRITE',
    GrantWriteACP: 'FULL_CONTROL',
};

// TO-DO: figure out mapping for these
const cannedAclAwsToGcp = {
    'aws-exec-read': false,
    'log-delivery-write': false,
};

const cannedAclGcp = {
    'private': true,
    'bucket-owner-read': true,
    'bucket-owner-full-control': true,
    'public-read': true,
    'public-read-write': true,
    'authenticated-read': true,
    'project-private': true,
};

const permissionsAwsToGcp = {
    FULL_CONTROL: 'FULL_CONTROL',
    WRITE: 'WRITE',
    READ: 'READ',
    WRITE_ACP: 'WRITE',
    READ_ACP: 'READ',
};

const gcpGrantTypes = {
    UserByEmail: 'emailAddress',
    UserById: 'id',
    GroupByEmail: 'emailAddress',
    GroupById: 'id',
    GroupByDomain: 'domain',
    AllAuthenticatedUsers: true,
    AllUsers: true,
};

const awsGrantMapping = {
    emailAddress: 'UserByEmail',
    id: 'UserById',
    uri: false,
};

const awsAcpMapping = {
    CanonicalUser: 'UserById',
    AmazonCustomerByEmail: 'UserByEmail',
    Group: false,
};

module.exports = {
    grantProps,
    cannedAclAwsToGcp,
    cannedAclGcp,
    permissionsAwsToGcp,
    gcpGrantTypes,
    awsGrantMapping,
    awsAcpMapping,
};
