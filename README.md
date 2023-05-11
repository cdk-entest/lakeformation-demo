---
title: lakeformation introduction
description: lakeformation introduction
author: haimtran
publishedDate: 10/05/2022
date: 2022-10-05
---

## Introduction

[GitHub] this notes show how IAM and LakeFormation togeter control access to data lake.

- Register an admin to LakeFormation
- Register S3 buckets to LakeFormation
- Grant (fine and graint) permissions to a Data Scientist
- Grant (fine and coarse) permissions to a Glue Role

## LakeFormation

Register an admin to LakeFormation, in this case the admin is CF execution role

```ts
this.lakeCdkAmin = new aws_lakeformation.CfnDataLakeSettings(
  this,
  "LakeFormationAdminSetting",
  {
    admins: [
      {
        dataLakePrincipalIdentifier: Fn.sub(
          (this.synthesizer as DefaultStackSynthesizer)
            .cloudFormationExecutionRoleArn
        ),
      },
    ],
  }
);
```

Create a S3 bucket for lake

```ts
const bucket = new aws_s3.Bucket(this, "S3LakeBucketDemo", {
  bucketName: props.s3LakeName,
  removalPolicy: RemovalPolicy.DESTROY,
  autoDeleteObjects: true,
});
```

Register data source buckets to lake

```ts
var registers: aws_lakeformation.CfnResource[] = [];
props.registerBuckets.map((bucket) => {
  registers.push(
    new aws_lakeformation.CfnResource(this, `RegsiterBucketToLake-${bucket}`, {
      resourceArn: `arn:aws:s3:::${bucket}`,
      useServiceLinkedRole: true,
    })
  );
});
```

## Grant Permissions to a Data Scientist

- Create an IAM user with password stored in Secret
- Coarse permissions by IAM
- Fine permissions by LakeFormation

Create an IAM user with passwrod stored in secret values

```ts
const secret = new aws_secretsmanager.Secret(this, `${props.userName}Secret`, {
  secretName: `${props.userName}Secret`,
  generateSecretString: {
    secretStringTemplate: JSON.stringify({ username: props.userName }),
    generateStringKey: "password",
  },
});

// create an iam user for data analyst (da)
const daUser = new aws_iam.User(this, `${props.userName}IAMUser`, {
  userName: props.userName,
  password: secret.secretValueFromJson("password"),
  passwordResetRequired: false,
});
```

DS need to save query result to Athena query result location

```ts
daUser.addToPolicy(
  new aws_iam.PolicyStatement({
    effect: Effect.ALLOW,
    actions: ["s3:*"],
    resources: [
      `arn:aws:s3:::${props.athenaResultBucket}/*`,
      `arn:aws:s3:::${props.athenaResultBucket}`,
    ],
  })
);
```

Coarse grant permission by IAM

```ts
daUser.addManagedPolicy(
  aws_iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess")
);
```

For DS to see Glue catalog databases, and table we need below permissions. In addition, DS need to get some permissions from LakeFormation

```ts
daUser.addToPolicy(
  new aws_iam.PolicyStatement({
    effect: Effect.ALLOW,
    actions: [
      "lakeformation:GetDataAccess",
      "glue:GetTable",
      "glue:GetTables",
      "glue:SearchTables",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetPartitions",
      "lakeformation:GetResourceLFTags",
      "lakeformation:ListLFTags",
      "lakeformation:GetLFTag",
      "lakeformation:SearchTablesByLFTags",
      "lakeformation:SearchDatabasesByLFTags",
    ],
    resources: ["*"],
  })
);
```

Fine grant permissions by LakeFormation, control access to tables, and database in Glue catalog. It is possible to control to column level and use Tag for scale.

```ts
const tablePermission = new aws_lakeformation.CfnPrincipalPermissions(
  this,
  `${props.userName}-ReadTableLake-2`,
  {
    permissions: props.databasePermissions,
    permissionsWithGrantOption: props.databasePermissions,
    principal: {
      dataLakePrincipalIdentifier: daUser.userArn,
    },
    resource: {
      table: {
        catalogId: this.account,
        databaseName: props.databaseName,
        tableWildcard: {},
      },
    },
  }
);

new aws_lakeformation.CfnPrincipalPermissions(
  this,
  `${props.userName}-ReadTableLake-4`,
  {
    permissions: props.databasePermissions,
    permissionsWithGrantOption: props.databasePermissions,
    principal: {
      dataLakePrincipalIdentifier: daUser.userArn,
    },
    resource: {
      database: {
        catalogId: this.account,
        name: props.databaseName,
      },
    },
  }
);
```

## Create Glue Role

- Coarse permissions with IAM
- Fine permissions with LakeFormation
- Permissions: read S3, write S3, create catalog tables

First, coarse permissions with IAM, need to attach AWSGlueServiceRole policy

```ts
const role = new aws_iam.Role(this, "LakeFormationWorkFlowRole", {
  roleName: "LakeFormationWorkFlowRole",
  assumedBy: new aws_iam.ServicePrincipal("glue.amazonaws.com"),
});

// remove this and check
role.addManagedPolicy(
  aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
    "service-role/AWSGlueServiceRole"
  )
);

role.addManagedPolicy(
  aws_iam.ManagedPolicy.fromAwsManagedPolicyName("CloudWatchAgentServerPolicy")
);
```

Second, Glue role need permissions to read data from source and write to a S3 lake bucket. It also need iam:PassRole and lakeformation:GetDataAccess

```ts
const policy = new aws_iam.Policy(this, "PolicyForLakeFormationWorkFlow", {
  policyName: "PolicyForLakeFormationWorkFlow",
  statements: [
    // pass iam role or follow name convention
    // AWSGlueServiceRoleDefault
    new aws_iam.PolicyStatement({
      actions: ["iam:PassRole", "iam:GetRole"],
      effect: Effect.ALLOW,
      resources: ["*"],
    }),
    // access s3
    new aws_iam.PolicyStatement({
      actions: ["s3:*"],
      effect: Effect.ALLOW,
      resources: [`${props.sourceBucketArn}/*`, `${props.destBucketArn}/*`],
    }),
    new aws_iam.PolicyStatement({
      actions: [
        "lakeformation:GetDataAccess",
        "lakeformation:GrantPermissions",
      ],
      effect: Effect.ALLOW,
      resources: ["*"],
    }),
  ],
});
policy.attachToRole(role);
```

Fine permission with LakeFormation so that the Glue role can create catalog tables in a database

```ts
const grant = new aws_lakeformation.CfnPrincipalPermissions(
  this,
  "GrantLocationPermissionGlueRole-1",
  {
    permissions: ["DATA_LOCATION_ACCESS"],
    permissionsWithGrantOption: ["DATA_LOCATION_ACCESS"],
    principal: {
      dataLakePrincipalIdentifier: role.roleArn,
    },
    resource: {
      dataLocation: {
        catalogId: this.account,
        resourceArn: `${props.destBucketArn}`,
      },
    },
  }
);
```

## Troubeshooting

- When creating a database in Lake Formation catalog, it is possible to specify location (S3 path of the database). Choose an Amazon S3 path for this database, which eliminates the need to grant data location permissions on catalog table paths that are this location’s children.

- Follow the convention name for GlueServiceRoleDefault

- Double check the Lake Formation Permission and Data Location when deploying and updating with CDK

## Reference

- [Lake Formation User Policy](https://docs.aws.amazon.com/lake-formation/latest/dg/initial-LF-setup.html)

- [IAMAllowedPrincipals](https://docs.aws.amazon.com/lake-formation/latest/dg/change-settings.html)

- [TableWildCard](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-lakeformation-permissions-tablewildcard.html)

- [Grant Table Permissions](https://docs.aws.amazon.com/lake-formation/latest/dg/granting-table-permissions.html)

- [Lake Formation with Glue Role](https://docs.aws.amazon.com/lake-formation/latest/dg/initial-LF-setup.html)

- [Glue Role Name Convention PassRole](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
